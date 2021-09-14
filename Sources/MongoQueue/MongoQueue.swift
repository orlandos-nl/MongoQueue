import MongoCore
import Foundation
import Meow

/// A QueuedTask is a Codable type that can execute the metadata it carries
///
/// These tasks can be queued to MongoDB, and an available process built on MongoQueue will pick up the task and run it
///
/// You cannot implement `_QueuedTask` yourself, but instead need to implement one of the derived protocols
public protocol _QueuedTask: Codable {
    /// The type of task being scheduled, defaults to your `Task.Type` name
    static var category: String { get }
    
    /// The amount of urgency your task has. Tasks with higher priority take precedence over lower priorities.
    /// When priorities are equal, the first-created task is executed fist.
    var priority: TaskPriority { get }
    
    /// An internal configuration object that MongoQueue uses to pass around internal metadata
    var configuration: _TaskConfiguration { get }
    
    /// The expected maximum duration of this task, defaults to 10 minutes
    var maxTaskDuration: TimeInterval { get }
    
    /// If a task is light & quick, you can enable paralellisation. A single worker can execute many parallelised tasks simultaneously.
    ///
    /// Defaults to `false`
//    var allowsParallelisation: Bool { get }
    
    /// Executes the task using the available metadata stored in `self`
    func execute() async throws
    
    /// Called when the task failed to execute. Provides an opportunity to decide the fate of this task
    ///
    /// - Parameters:
    ///     - totalAttempts: The amount of attempts thus far, including the failed one`
    func onExecutionFailure(totalAttempts: Int) async throws -> TaskExecutionFailureAction
}

extension _QueuedTask {
    public static var category: String { String(describing: Self.self) }
    public var priority: TaskPriority { .normal }
    public var maxTaskDuration: TimeInterval { 10 * 60 }
//    public var allowsParallelisation: Bool { false }
}

public struct TaskExecutionFailureAction {
    enum _Raw {
        case dequeue
        case retry(maxAttempts: Int?)
        case retryAfter(TimeInterval, maxAttempts: Int?)
    }
    
    let raw: _Raw
    
    public static func dequeue() -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .dequeue)
    }
    
    public static func retry(maxAttempts: Int?) -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .retry(maxAttempts: maxAttempts))
    }
    
    public static func retryAfter(_ interval: TimeInterval, maxAttempts: Int?) -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .retryAfter(interval, maxAttempts: maxAttempts))
    }
}

/// The current status of your task
public struct TaskStatus {
    internal enum _Raw: String, Codable {
        case scheduled
        case suspended
        case executing
    }
    
    internal let raw: _Raw
    
    /// The task is scheduled, and is ready for execution
    public static let scheduled = TaskStatus(raw: .scheduled)
    
    /// The task has been suspended until further action
    public static let suspended = TaskStatus(raw: .suspended)
    
    /// The task is currently executing
    public static let executing = TaskStatus(raw: .executing)
}

public struct TaskPriority {
    internal enum _Raw: Int, Codable {
        case relaxed = -2, lower = -1, normal = 0, higher = 1, urgent = 2
    }
    
    internal let raw: _Raw
    
    /// Take your time, it's expected to take a while
    public static let relaxed = TaskPriority(raw: .relaxed)
    
    /// Not as urgent as regular user actions, but please do not take all the time in the world
    public static let lower = TaskPriority(raw: .lower)
    
    /// Regular user actions
    public static let normal = TaskPriority(raw: .normal)
    
    /// This is needed fast, think of real-time communication
    public static let higher = TaskPriority(raw: .higher)
    
    /// THIS SHOULDN'T WAIT
    /// Though, if something is to be executed _immediately_, you probably shouldn't use a job queue
    public static let urgent = TaskPriority(raw: .urgent)
}

public struct _TaskConfiguration {
    internal enum _TaskConfiguration {
        case scheduled(ScheduledTaskConfiguration)
    }
    
    internal var value: _TaskConfiguration
    
    internal init(value: _TaskConfiguration) {
        self.value = value
    }
}

// - MARK: Scheduled

public protocol ScheduledTask: _QueuedTask {
    /// The date that you want this to be executed (delay)
    /// If you want it to be immediate, use `Date()`
    var scheduledDate: Date { get }
    var executeBefore: Date? { get }
}

extension ScheduledTask {
    public var executeBefore: Date? { nil }
    
    public var configuration: _TaskConfiguration {
        let scheduled = ScheduledTaskConfiguration(
            scheduledDate: scheduledDate,
            executeBefore: executeBefore
        )
        return _TaskConfiguration(value: .scheduled(scheduled))
    }
}

public struct ScheduledTaskConfiguration: Codable {
    let scheduledDate: Date
    let executeBefore: Date?
}

// TODO: Recurring Jobs

public protocol RecurringTask: _QueuedTask {
    
}

// - MARK: Global API

public enum MongoQueueError: Error {
    public enum TaskExecutionReason {
        case failedToClaim
        case taskError(Error)
    }
    
    case alreadyStarted
    case taskCreationFailed
    case taskExecutionFailed(reason: TaskExecutionReason)
    case unknownTaskCategory
    case reschedulingFailedTaskFailed
    case dequeueTaskFailed
}

internal struct KnownType {
    let category: String
    let executeTask: (inout TaskModel) async throws -> ()
    
    init<T: _QueuedTask>(
        type: T.Type,
        collection: MongoCollection.Async
    ) {
        self.category = type.category
        self.executeTask = { task in
            try await KnownType.executeTask(
                &task,
                collection: collection,
                ofType: type
            )
        }
    }
    
    private static func executeTask<T: _QueuedTask>(
        _ task: inout TaskModel,
        collection: MongoCollection.Async,
        ofType type: T.Type
    ) async throws {
        // TODO: WriteConcern majority
//        var writeConcern = WriteConcern()
//        writeConcern.acknowledgement = .majority
        
        task.status = .executing
        guard try await collection.upsertEncoded(task, where: "_id" == task._id).updatedCount == 1 else {
            throw MongoQueueError.taskExecutionFailed(reason: .failedToClaim)
        }
        
        let metadata = try BSONDecoder().decode(type, from: task.metadata)
        
        switch try task.readConfiguration().value {
        case .scheduled(let task):
            if let executeBefore = task.executeBefore, executeBefore < Date() {
                // Task didn't execute
                return
            }
        }
        
        do {
            task.attempts += 1
            try await metadata.execute()
            // TODO: We assume this succeeds, but what if it does not?
            _ = try? await collection.deleteOne(where: "_id" == task._id)
            return
        } catch {
            let onFailure = try await metadata.onExecutionFailure(totalAttempts: task.attempts)
            
            switch onFailure.raw {
            case .dequeue:
                guard try await collection.deleteOne(where: "_id" == task._id).deletes == 1 else {
                    throw MongoQueueError.dequeueTaskFailed
                }
            case .retry(maxAttempts: let maxAttempts):
                if let maxAttempts = maxAttempts, task.attempts >= maxAttempts {
                    guard try await collection.deleteOne(where: "_id" == task._id).deletes == 1 else {
                        throw MongoQueueError.dequeueTaskFailed
                    }
                } else {
                    guard try await collection.upsertEncoded(task, where: "_id" == task._id).updatedCount == 1 else {
                        throw MongoQueueError.reschedulingFailedTaskFailed
                    }
                }
            case .retryAfter(let nextInterval, maxAttempts: let maxAttempts):
                if let maxAttempts = maxAttempts, task.attempts >= maxAttempts {
                    try await collection.deleteOne(where: "_id" == task._id)
                } else {
                    task.executeAfter = Date().addingTimeInterval(nextInterval)
                }
            }
            
            // Throw the initial error
            throw error
        }
    }
}

enum TaskExecutionResult {
    case noneExecuted
    case taskSuccessful
    case taskFailure(Error)
}

public final class MongoQueue {
//    private final actor ExecutingTasks {
//        var runningTasks = 0
//
//        func increment() {
//            runningTasks += 1
//        }
//
//        func decrement() {
//            runningTasks -= 1
//            assert(runningTasks >= 0)
//        }
//    }
    
    private let collection: MongoCollection.Async
    private var knownTypes = [KnownType]()
    private var started = false
    private var serverHasData = true
    public var pollingFrequencyMs: Int = 5000
//    private let executingTasks = ExecutingTasks()
    
    public init(collection: MongoCollection) {
        self.collection = collection.async
    }
    
    public init(collection: MongoCollection.Async) {
        self.collection = collection
    }
    
    public func registerTask<T: _QueuedTask>(_ type: T.Type) {
        knownTypes.append(KnownType(type: type, collection: collection))
    }
    
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
    
    @discardableResult
    public func suspendTasks<T: _QueuedTask, Q: MongoKittenQuery>(ofType type: T.Type, where filter: Q) async throws -> UpdateReply {
        try await suspendTasks(ofType: type, where: filter.makeDocument())
    }
    
    @discardableResult
    public func unsuspendTasks<T: _QueuedTask, Q: MongoKittenQuery>(ofType type: T.Type, where filter: Q) async throws -> UpdateReply {
        try await unsuspendTasks(ofType: type, where: filter.makeDocument())
    }
    
    func runNextTask() async throws -> TaskExecutionResult {
        let context = try BSONEncoder().encode(TaskModel.ExecutingContext())
        var writeConcern = WriteConcern()
        writeConcern.acknowledgement = .majority
        
        let reply = try await collection.nio.findOneAndUpdate(
            where: [
                "status": TaskStatus.scheduled.raw.rawValue,
            ],
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
                "creationDate": .ascending
            ])
            .writeConcern(writeConcern)
            .execute()
            .get()
        
        guard let taskDocument = reply.value else {
            // No task found
            return .noneExecuted
        }
        
        var task = try BSONDecoder().decode(TaskModel.self, from: taskDocument)
        guard let knownType = knownTypes.first(where: { $0.category == task.category }) else {
            throw MongoQueueError.unknownTaskCategory
        }
        
        do {
            try await knownType.executeTask(&task)
            return .taskSuccessful
        } catch {
            return .taskFailure(error)
        }
    }
    
    // TODO: ReadConcern majority in >= MongoDB 4.2
    public func start() throws {
        if started {
            throw MongoQueueError.alreadyStarted
        }
        
        started = true
        
        if let wireVersion = collection.nio.database.pool.wireVersion, wireVersion.supportsCollectionChangeStream {
            Task.detached {
                try await self.startChangeStreamTicks()
            }
        } else {
            Task.detached {
                try await self.sleepBasedTick()
            }
        }
    }
    
    private func startChangeStreamTicks() async throws {
        // Using change stream cursor based polling
        var options = ChangeStreamOptions()
        options.maxAwaitTimeMS = 50
        var cursor = try await collection.watch(options: options, as: TaskModel.self)
        cursor.setGetMoreInterval(to: .seconds(5))
        cursor.forEach { change in
            if change.operationType == .insert || change.operationType == .update || change.operationType == .replace {
                // Dataset changed, retry
                if self.serverHasData {
                    self.serverHasData = true
                    Task.detached {
                        try await self.cursorInitiatedTick()
                    }
                }
            }
            
            return self.started
        }
        
        Task.detached {
            try await self.cursorInitiatedTick()
        }
        
        do {
            // Ideally, this is where we stay
            try await cursor.awaitClose()
        } catch {}
        
        if self.started {
            // Restart Change Stream
            try await startChangeStreamTicks()
        }
    }
    
    private func cursorInitiatedTick() async throws {
        do {
            switch try await self.runNextTask() {
            case .taskSuccessful, .taskFailure:
                Task.detached {
                    try await self.cursorInitiatedTick()
                }
            case .noneExecuted:
                serverHasData = false
            }
        } catch {
            // Task execution failed due to a MongoDB error
            // Otherwise the return type would specify the task status
            serverHasData = false
        }
    }
    
    private func sleepBasedTick() async throws {
        if !started {
            return
        }
        
        if !serverHasData {
            await Task.sleep(UInt64(pollingFrequencyMs) * 1_000_000)
        }
        
        do {
            if case .noneExecuted = try await self.runNextTask() {
                serverHasData = false
            } else {
                serverHasData = true
            }
        } catch {
            // Task execution failed due to a MongoDB error
            // Otherwise the return type would specify the task status
            serverHasData = false
        }
        
        Task.detached {
            try await self.sleepBasedTick()
        }
    }
    
    public func queueTask<T: _QueuedTask>(_ task: T) async throws {
        assert(knownTypes.contains(where: { $0.category == T.category }))
        
        let model = try TaskModel(representing: task)
        
        // TODO: WriteConcern majority
//        var writeConcern = WriteConcern()
//        writeConcern.acknowledgement = .majority
        
        let reply = try await collection.insertEncoded(model)
        guard reply.insertCount == 1 else {
            throw MongoQueueError.taskCreationFailed
        }
    }
}

internal struct TaskModel: Codable {
    let _id: ObjectId
    
    /// Contains `Task.name`
    let category: String
    
    let creationDate: Date
    let priority: TaskPriority._Raw
    var executeAfter: Date
    var executeBefore: Date?
    var attempts: Int
    var status: TaskStatus._Raw
    var metadata: Document
    
    struct ExecutingContext: Codable {
        /// Used to represent when the task was first started. Normally it's equal to `executionStartDate`
        /// But when a task takes an unexpectedly long amount of time, the two values will be different
        let startDate: Date
        
        /// If `status == .executing`, this marks the start timestamp
        /// This allows tasks to be rebooted if the executioner process crashed
        /// If the current date exceeds `executionStartDate + maxTaskDuration`, the task likely crashed
        /// The executioner of the task **MUST** refresh this date at least every `maxTaskDuration` interval to ensure other executioners don't pick up on the task
        var lastUpdate: Date
        
        init() {
            let now = Date()
            self.startDate = now
            self.lastUpdate = now
        }
        
        mutating func updateActivity() {
            lastUpdate = Date()
        }
    }
    
    var execution: ExecutingContext?
    let maxTaskDuration: TimeInterval
//    let allowsParallelisation: Bool
    
    private enum ConfigurationType: String, Codable {
        case scheduled
    }
    
    private let configurationType: ConfigurationType
    private let configuration: Document
    
    init<T: _QueuedTask>(representing task: T) throws {
        self._id = ObjectId()
        self.category = T.category
        self.priority = task.priority.raw
        self.attempts = 0
        self.creationDate = Date()
        self.status = .scheduled
        self.metadata = try BSONEncoder().encode(task)
        self.maxTaskDuration = task.maxTaskDuration
//        self.allowsParallelisation = task.allowsParallelisation
        
        switch task.configuration.value {
        case .scheduled(let configuration):
            self.configurationType = .scheduled
            self.executeAfter = configuration.scheduledDate
            self.executeBefore = configuration.executeBefore
            self.configuration = try BSONEncoder().encode(configuration)
        }
    }
    
    func readConfiguration() throws -> _TaskConfiguration {
        switch configurationType {
        case .scheduled:
            return try _TaskConfiguration(
                value: .scheduled(
                    BSONDecoder().decode(ScheduledTaskConfiguration.self, from: configuration)
                )
            )
        }
    }
}
