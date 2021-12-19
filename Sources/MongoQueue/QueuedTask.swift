import Foundation

/// A QueuedTask is a Codable type that can execute the metadata it carries
///
/// These tasks can be queued to MongoDB, and an available process built on MongoQueue will pick up the task and run it
///
/// You cannot implement `_QueuedTask` yourself, but instead need to implement one of the derived protocols
public protocol _QueuedTask: Codable {
    associatedtype ExecutionContext
    
    /// The type of task being scheduled, defaults to your `Task.Type` name
    static var category: String { get }
    
    ///
    var group: String? { get }
    
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
    func execute(withContext context: ExecutionContext) async throws
    
    func onDequeueTask(_ task: TaskModel, withContext context: ExecutionContext, inQueue queue: MongoQueue) async throws
    
    /// Called when the task failed to execute. Provides an opportunity to decide the fate of this task
    ///
    /// - Parameters:
    ///     - totalAttempts: The amount of attempts thus far, including the failed one`
    func onExecutionFailure(failureContext: QueuedTaskFailure<ExecutionContext>) async throws -> TaskExecutionFailureAction
}

extension _QueuedTask {
    public static var category: String { String(describing: Self.self) }
    public var priority: TaskPriority { .normal }
    public var group: String? { nil }
    public var maxTaskDuration: TimeInterval { 10 * 60 }
//    public var allowsParallelisation: Bool { false }
}
