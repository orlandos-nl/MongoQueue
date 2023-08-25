import Logging
import MongoCore
import Foundation
import Meow

/// A MongoQueue is a queue that uses MongoDB as a backend for storing tasks. It is designed to be used in a distributed environment.
public final class MongoQueue {
    internal let collection: MongoCollection
    internal let logger = Logger(label: "org.openkitten.mongo-queues")
    private var knownTypes = [KnownType]()
    private var started = false
    private var serverHasData = true
    private var maxParallelJobs = 1
    private var task: Task<Void, Never>?
    public var newTaskPollingFrequency = NIO.TimeAmount.milliseconds(1000)

    /// The frequency at which the queue will check for stalled tasks. Defaults to 30 seconds. This is the time after which a task is considered stalled and will be requeued.
    public var stalledTaskPollingFrequency = NIO.TimeAmount.seconds(30)
    
    /// Creates a new MongoQueue with the given collection as a backend for storing tasks.
    public init(collection: MongoCollection) {
        self.collection = collection
    }

    public func setMaxParallelJobs(to max: Int) {
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
        
        // If another instance crashed, causing a stale task, this ensures the task gets requeued
        task = Task { [weak self] in
            repeat {
                guard let queue = self else {
                    return
                }
                
                _ = try? await queue.findAndRequeueStaleTasks()
                _ = try? await Task.sleep(nanoseconds: UInt64(stalledTaskPollingFrequency.nanoseconds))
            } while !Task.isCancelled
        }
        
        let pool = collection.database.pool
        
        if
            let wireVersion = await pool.wireVersion,
            wireVersion.supportsCollectionChangeStream,
            let hosts = try await pool.next(for: .writable).serverHandshake?.hosts,
            hosts.count > 0
        {
            try await cursorInitiatedTick()
            try await self.startChangeStreamTicks()
        } else {
            try await self.sleepBasedTick()
        }
    }
    
    /// Stops the queue. This will return immediately and the queue will stop running.
    public func shutdown() {
        self.started = false
    }
    
    private func startChangeStreamTicks() async throws {
        // Using change stream cursor based polling
        var options = ChangeStreamOptions()
        options.maxAwaitTimeMS = 50
        var cursor = try await collection.watch(options: options, type: TaskModel.self)
        cursor.setGetMoreInterval(to: newTaskPollingFrequency)
        let iterator = cursor.forEach { change in
            if change.operationType == .insert || change.operationType == .update || change.operationType == .replace {
                // Dataset changed, retry
                if !self.serverHasData {
                    self.serverHasData = true
                    Task {
                        try await self.cursorInitiatedTick()
                    }
                }
            }
            
            return self.started
        }
        
        // Kick off the first tick, because we might immediately have work
        try await self.cursorInitiatedTick()
        
        // The change stream only observes _changes_ to datasets
        // That allows us to respond to new tasks rapidly
        // We still want background ticks so that a 
        let backgroundTicks = Task {
            try await self.sleepBasedTick()
        }
        
        do {
            // Ideally, this is where we stay
            try await iterator.value
        } catch {}
        
        backgroundTicks.cancel()
        
        if self.started {
            // Restart Change Stream
            try await startChangeStreamTicks()
        }
    }
    
    private func cursorInitiatedTick() async throws {
        do {
            switch try await self.runNextTask() {
            case .taskFailure(let error):
                logger.error("\(error)")
                fallthrough
            case .taskSuccessful:
                Task {
                    try await self.cursorInitiatedTick()
                }
            case .noneExecuted:
                serverHasData = false
            }
        } catch {
            // Task execution failed due to a MongoDB error
            // Otherwise the return type would specify the task status
            logger.error("\(error)")
            serverHasData = false
        }
    }
    
    private func sleepBasedTick() async throws {
        while started {
            if !serverHasData {
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
    
    /// Queues a task for execution. The task will be executed after the specified Date.
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
        
        serverHasData = true
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
        self.logger.info("Dequeueing stale task id \(task._id) of type \(task.category)")
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
