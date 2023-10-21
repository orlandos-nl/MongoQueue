import Foundation

/// A QueuedTask is a Codable type that can execute the metadata it carries
///
/// These tasks can be queued to MongoDB, and an available process built on MongoQueue will pick up the task and run it
///
/// You cannot implement this protocol directly, but instead need to implement one of the derived protocols.
/// This can be either ``ScheduledTask`` or ``RecurringTask``, but not both at the same time.
public protocol _QueuedTask: Codable {
    associatedtype ExecutionContext = Void
    
    /// The type of task being scheduled, defaults to your Type's name. This chosen value may only be associated with one type at a time.
    static var category: String { get }
    
    /// A group represents custom information that you can query for when disabling or deleting tasks.
    ///
    /// For example, a user's ID can be associated with a `group`, so that all the tasks that provide information to this user can be cleaned up in bulk when a user is deleted from the system.
    var group: String? { get }
    
    /// The amount of urgency your task has. Tasks with higher priority take precedence over lower priorities.
    /// When priorities are equal, the earlier-created task is executed first.
    var priority: TaskPriority { get }
    
    /// An internal configuration object that MongoQueue uses to pass around internal metadata
    ///
    /// - Warning: Do not implement or use this yourself, if you need this hook let us know
    var configuration: _TaskConfiguration { get }
    
    /// The expected maximum duration of this task, defaults to 10 minutes
    var maxTaskDuration: TimeInterval { get }
    
    /// If a task is light & quick, you can enable paralellisation. A single worker can execute many parallelised tasks simultaneously.
    ///
    /// Defaults to `false`
//    var allowsParallelisation: Bool { get }
    
    /// Executes the task using the stored properties in `self`. `ExecutionContext` can be any instance of your choosing, and is used as a means to execute the task. In the case of an newsletter task, this would be the email client.
    mutating func execute(withContext context: ExecutionContext) async throws

    /// - Warning: Do not implement this method yourself, if you need this hook let us know
    func _onDequeueTask(_ task: TaskModel, withContext context: ExecutionContext, inQueue queue: MongoQueue) async throws -> _DequeueResult
    
    /// Called when the task failed to execute. Provides an opportunity to decide the fate of this task
    ///
    /// - Parameters:
    ///     - totalAttempts: The amount of attempts thus far, including the failed one`
    func onExecutionFailure(failureContext: QueuedTaskFailure<ExecutionContext>) async throws -> TaskExecutionFailureAction
}

/// A publically non-initializable type that prevents users from overriding `onDequeueTask`
public struct _DequeueResult {}

extension _QueuedTask {
    public static var category: String { String(describing: Self.self) }
    public var priority: TaskPriority { .normal }
    public var group: String? { nil }
    public var maxTaskDuration: TimeInterval { 10 * 60 }
//    public var allowsParallelisation: Bool { false }
}
