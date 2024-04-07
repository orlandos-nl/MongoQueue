import Logging
import MongoCore
import NIOConcurrencyHelpers
import Foundation
import Meow

/// A MongoQueue is a queue that uses MongoDB as a backend for storing tasks. It is designed to be used in a distributed environment.
///
/// 1. First, connect to MongoDB and create the MongoQueue.
/// 2. Then, register your tasks _with_ ExecutionContext.
/// 3. Finally, start the job queue.
///
/// ```swift
/// import MongoKitten
///
/// let db = try await MongoDatabase.connect(to: "mongodb://localhost/my-db")
/// let queue = MongoQueue(db["job-queue"])
/// queue.registerTask(Reminder.self, context: executionContext)
/// // Run the queue until it's stopped or a cancellation is received
/// try await queue.run()
/// ```
///
/// To insert a new task into the queue:
///
/// ```swift
/// try await queue.queueTask(Reminder(username: "Joannis"))
/// ```
public final class MongoQueue: @unchecked Sendable {
    public struct Option: Hashable {
        internal enum _Option: Hashable {
            case uniqueKeysEnabled
        }

        internal let raw: _Option

        public static let enableUniqueKeys = Option(raw: .uniqueKeysEnabled)
    }

    internal let collection: MongoCollection
    internal let logger = Logger(label: "org.openkitten.mongo-queues")
    private var knownTypes = [KnownType]()
    private let _started = NIOLockedValueBox(false)
    private var started: Bool {
        get { _started.withLockedValue { $0 } }
        set { _started.withLockedValue  { $0 = newValue } }
    }
    private let _serverHasData = NIOLockedValueBox(true)
    private var serverHasData: Bool {
        get { _serverHasData.withLockedValue { $0 } }
        set { _serverHasData.withLockedValue  { $0 = newValue } }
    }
    private let checkServerNotifications = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(2))
    private var maxParallelJobs = 1
    public var newTaskPollingFrequency = NIO.TimeAmount.milliseconds(1000)
    public let options: Set<Option>

    /// The frequency at which the queue will check for stalled tasks. Defaults to 30 seconds. This is the time after which a task is considered stalled and will be requeued.
    public var stalledTaskPollingFrequency = NIO.TimeAmount.seconds(30)
    
    /// Creates a new MongoQueue with the given collection as a backend for storing tasks.
    public init(collection: MongoCollection, options: Set<Option>) {
        self.collection = collection
        self.options = options
    }

    public init(collection: MongoCollection) {
        self.collection = collection
        self.options = []
    }

    private func assertNotRunning(_ function: String = #function) {
        assert(!started, "Do not use `\(function)` while the queue is running")
    }

    public func setMaxParallelJobs(to max: Int) {
        assertNotRunning()
        maxParallelJobs = max
    }
    
    /// Registers a task type to the queue. This is required for the queue to be able to perform the task.
    /// - Parameters:
    ///  - type: The type of the task to register (e.g. `MyTask.self`)
    /// - context: The context in which the task should be executed in (e.g. a `Request`)
    public func registerTask<T: _QueuedTask>(
        _ type: T.Type,
        context: T.ExecutionContext
    ) {
        assertNotRunning()
        knownTypes.append(KnownType(type: type, queue: self, logger: logger, context: context))
    }
    
    /// Suspends all tasks of the given type that match the given filter.
    /// - Parameters:
    /// - type: The type of the task to suspend (e.g. `MyTask.self`)
    /// - filter: A filter to match tasks against. Defaults to an empty filter, which will match all tasks of the given type.
    /// - Returns: The number of tasks that were suspended.
    @discardableResult
    public func suspendTasks<T: _QueuedTask>(ofType type: T.Type, where filter: Document = [:]) async throws -> UpdateReply {
        try await collection.updateMany(
            where: [
                "status": TaskStatus.scheduled.raw.rawValue,
                "metadata": filter
            ],
             setting: [
                "status": TaskStatus.suspended.raw.rawValue
             ],
             unsetting: nil
        )
    }
    
    /// Unsuspends all tasks of the given type that match the given filter.
    /// - Parameters:
    /// - type: The type of the task to unsuspend (e.g. `MyTask.self`)
    /// - filter: A filter to match tasks against. Defaults to an empty filter, which will match all tasks of the given type.
    /// - Returns: The number of tasks that were unsuspended.
    @discardableResult
    public func unsuspendTasks<T: _QueuedTask>(ofType type: T.Type, where filter: Document = [:]) async throws -> UpdateReply {
        try await collection.updateMany(
            where: [
                "status": TaskStatus.suspended.raw.rawValue,
                "metadata": filter
            ],
             setting: [
                "status": TaskStatus.scheduled.raw.rawValue
             ],
             unsetting: nil
        )
    }
    
    /// Suspends all tasks of the given type that match the given filter.
    /// - Parameters:
    /// - type: The type of the task to suspend (e.g. `MyTask.self`)
    /// - filter: A filter to match tasks against. Defaults to an empty filter, which will match all tasks of the given type.
    /// - Returns: The number of tasks that were suspended.
    @discardableResult
    public func suspendTasks<T: _QueuedTask, Q: MongoKittenQuery>(ofType type: T.Type, where filter: Q) async throws -> UpdateReply {
        try await suspendTasks(ofType: type, where: filter.makeDocument())
    }
    
    /// Unsuspends all tasks of the given type that match the given filter.
    /// - Parameters:
    /// - type: The type of the task to unsuspend (e.g. `MyTask.self`)
    /// - filter: A filter to match tasks against. Defaults to an empty filter, which will match all tasks of the given type.
    /// - Returns: The number of tasks that were unsuspended.
    @discardableResult
    public func unsuspendTasks<T: _QueuedTask, Q: MongoKittenQuery>(ofType type: T.Type, where filter: Q) async throws -> UpdateReply {
        try await unsuspendTasks(ofType: type, where: filter.makeDocument())
    }
    
    func runNextTask() async throws -> TaskExecutionResult {
        let context = try BSONEncoder().encode(TaskModel.ExecutingContext())
        var writeConcern = WriteConcern()
        writeConcern.acknowledgement = .majority
        
        var filter: Document = "status" == TaskStatus.scheduled.raw.rawValue
        let executeAfterFilter: Document = "executeAfter" <= Date()
        filter = (filter && executeAfterFilter).makeDocument()
        
        let reply = try await collection.findOneAndUpdate(
            where: filter,
            to: [
                "$set": [
                    "status": TaskStatus.executing.raw.rawValue,
                    "execution": context
                ] as Document
            ],
            returnValue: .modified
        )
        .sort([
            "priority": .descending,
            "executeBefore": .ascending,
            "creationDate": .ascending
        ])
        .writeConcern(writeConcern)
        .execute()
        
        guard let taskDocument = reply.value else {
            // No task found
            return .noneExecuted
        }
        
        var task = try BSONDecoder().decode(TaskModel.self, from: taskDocument)
        guard let knownType = knownTypes.first(where: { $0.category == task.category }) else {
            throw MongoQueueError.unknownTaskCategory
        }
        
        do {
            try await knownType.performTask(&task)
            return .taskSuccessful
        } catch {
            return .taskFailure(error)
        }
    }
    
    /// Runs the queue in the background. This will return immediately and the queue will run in the background.
    @discardableResult
    public func runInBackground() throws -> Task<Void, Error> {
        if started {
            throw MongoQueueError.alreadyStarted
        }
        
        return Task {
            try await self.run()
        }
    }

    public func runUntilEmpty() async throws {
        try await ensureIndexes()

        struct TickResult {
            var errors: [Error]
            var tasksRan: Int
        }

        while !Task.isCancelled {
            let result = try await withThrowingTaskGroup(
                of: TaskExecutionResult.self,
                returning: TickResult.self
            ) { group in
                for _ in 0..<maxParallelJobs {
                    group.addTask {
                        try await self.runNextTask()
                    }
                }

                return try await group.reduce(TickResult(errors: [], tasksRan: 0)) { result, taskReslt in
                    var result = result

                    switch taskReslt {
                    case .taskSuccessful:
                        result.tasksRan += 1
                    case .taskFailure(let error):
                        result.tasksRan += 1
                        result.errors.append(error)
                    case .noneExecuted:
                        self.serverHasData = false
                    }

                    return result
                }
            }

            for error in result.errors {
                logger.error("\(error)")
            }

            if result.tasksRan < self.maxParallelJobs {
                return
            }
        }
    }
    
    /// Runs the queue. This will return when the queue is stopped. 
    public func run() async throws {
        if started {
            throw MongoQueueError.alreadyStarted
        }
        
        started = true

        try await ensureIndexes()
        try await withThrowingTaskGroup(of: Void.self) { taskGroup in
            taskGroup.addTask {
                while !Task.isCancelled {
                    _ = try? await self.findAndRequeueStaleTasks()
                    try await Task.sleep(nanoseconds: UInt64(self.stalledTaskPollingFrequency.nanoseconds))
                }
            }
            
            taskGroup.addTask {
                let pool = self.collection.database.pool

                if
                    let wireVersion = await pool.wireVersion,
                    wireVersion.supportsCollectionChangeStream,
                    let hosts = try await pool.next(for: .writable).serverHandshake?.hosts,
                    hosts.count > 0
                {
                    // Change stream ticks only work on change-stream supporting MongoDB versions
                    // And require the setup to be a replica set
                    try await self.startChangeStreamTicks()
                } else {
                    try await self.startSleepBasedTicks()
                }
            }

            taskGroup.addTask {
                for await () in self.checkServerNotifications.stream {
                    try? await self.tick()
                }
            }

            try await taskGroup.next()
            taskGroup.cancelAll()
        }
    }
    
    /// Stops the queue. This will return immediately and the queue will stop running.
    public func shutdown() {
        self.started = false
        self.checkServerNotifications.continuation.finish()
    }

    /// - Note: Gets called automatically when you `run` the application.
    public func ensureIndexes() async throws {
        if options.contains(.enableUniqueKeys) {
            var uniqueKeyIndex = CreateIndexes.Index(
                named: "unique-task-by-key",
                keys: [
                    "uniqueKey": 1,
                    "category": 1
                ]
            )
            uniqueKeyIndex.unique = true
            uniqueKeyIndex.partialFilterExpression = ["status": ["$in": ["scheduled", "executing"]]]
            try await collection.createIndexes([uniqueKeyIndex])
        }
    }

    private func signalHasData() {
        serverHasData = true
        checkServerNotifications.continuation.yield()
    }

    private func startChangeStreamTicks() async throws {
        while !Task.isCancelled, self.started {
            do {
                // Kick off the first tick, because we might already have backlog
                try await self.tick()

                try await withThrowingTaskGroup(of: Void.self) { taskGroup in
                    taskGroup.addTask {
                        try await self.startSleepBasedTicks()
                    }
                    taskGroup.addTask { [collection] in
                        // Using change stream cursor based polling
                        var options = ChangeStreamOptions()
                        options.maxAwaitTimeMS = 50
                        var changeStream = try await collection.watch(options: options, type: TaskModel.self)
                        changeStream.setGetMoreInterval(to: self.newTaskPollingFrequency)
                        for try await change in changeStream where change.operationType.requiresPolling {
                            // Dataset changed, retry
                            self.signalHasData()
                        }
                    }

                    // Wait for one of the two to stop or fail
                    // Then unwind the rest
                    try await taskGroup.next()
                    taskGroup.cancelAll()
                }
            } catch {}
        }
    }
    
    private func tick() async throws {
        do {
            repeat {
                switch try await self.runNextTask() {
                case .taskFailure(let error):
                    logger.debug("Failed to run task: \(error)")
                    fallthrough
                case .taskSuccessful:
                    serverHasData = true
                case .noneExecuted:
                    serverHasData = false
                }
            } while !Task.isCancelled && serverHasData
        } catch {
            // Task execution failed due to a MongoDB error
            // Otherwise the return type would specify the task status
            logger.error("Failed to get next task: \(error)")
            serverHasData = false
        }
    }
    
    /// Used when there's no possibility of opening a ChangeStream
    private func startSleepBasedTicks() async throws {
        while started && !Task.isCancelled {
            if !serverHasData {
                // Delay a bit, so we don't overload MongoDB with useless requests
                try await Task.sleep(nanoseconds: UInt64(newTaskPollingFrequency.nanoseconds))
            }

            do {
                try await runUntilEmpty()
            } catch {
                // Task execution failed due to a MongoDB error
                // Otherwise the return type would specify the task status
                logger.error("\(error)")
                serverHasData = false
            }
        }
    }
    
    /// Queues a task for execution.
    public func queueTask<T: _QueuedTask>(_ task: T) async throws {
        assert(
            knownTypes.contains(where: { $0.category == T.category }),
            "Task `\(T.self)` is not a known type in MongoQueue. Did you forget to register it using `queue.registerTask`?"
        )
        
        let model = try TaskModel(representing: task)
        
        var writeConcern = WriteConcern()
        writeConcern.acknowledgement = .majority
        
        let reply = try await collection.insertEncoded(model, writeConcern: writeConcern)
        guard reply.insertCount == 1 else {
            throw MongoQueueError.taskCreationFailed
        }
        
        self.signalHasData()
    }
    
    private func findAndRequeueStaleTasks() async throws {
        try await withThrowingTaskGroup(of: Void.self) { group in
            for type in knownTypes {
                group.addTask {
                    let executingTasks = self.collection.find(
                        "category" == type.category && "status" == TaskStatus.executing.raw.rawValue
                    ).decode(TaskModel.self)

                    for try await task in executingTasks {
                        if
                            let lastUpdateDate = task.execution?.lastUpdate,
                            lastUpdateDate.addingTimeInterval(task.maxTaskDuration) <= Date()
                        {
                            async let _ = await self.requeueStaleTask(task)
                        }
                    }
                }
            }

            try await group.waitForAll()
        }
    }

    private func requeueStaleTask(_ task: TaskModel) async {
        self.logger.debug("Dequeueing stale task id \(task._id) of type \(task.category)")
        do {
            _ = try await self.collection.findOneAndUpdate(where: "_id" == task._id, to: [
                "$set": [
                    "status": TaskStatus.scheduled.raw.rawValue,
                    "execution": Null()
                ] as Document
            ]).execute()
        } catch {
            self.logger.error("Failed to dequeue stale task id \(task._id) of type \(task.category)")
        }
    }
}

extension ChangeStreamNotification.OperationType {
    internal var requiresPolling: Bool {
        switch self {
        case .insert, .update, .replace:
            return true
        case .delete, .invalidate, .drop, .dropDatabase, .rename:
            return false
        }
    }
}
