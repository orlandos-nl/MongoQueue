import Foundation

/// The action that should be taken when a task is done executing.
public struct TaskRemovalAction {
    enum _Raw {
        case dequeue, softDelete
    }
    
    let raw: _Raw
    
    /// Dequeues the task from the queue, removing it from the queue.
    public static func dequeue() -> TaskRemovalAction {
        TaskRemovalAction(raw: .dequeue)
    }
    
    /// Soft-deletes the task from the queue, marking it as dequeued, but keeping it in the queue.
    public static func softDelete() -> TaskRemovalAction {
        TaskRemovalAction(raw: .softDelete)
    }
}

/// The action that should be taken when a task fails to execute.
public struct TaskExecutionFailureAction {
    enum _Raw {
        case removal(TaskRemovalAction)
        case retry(maxAttempts: Int?, action: TaskRemovalAction)
        case retryAfter(TimeInterval, maxAttempts: Int?, action: TaskRemovalAction)
    }
    
    let raw: _Raw
    
    /// Dequeues the task from the queue, removing it from the queue.
    public static func dequeue() -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .removal(.dequeue()))
    }
    
    /// Soft-deletes the task from the queue, marking it as dequeued, but keeping it in the queue.
    public static func softDelete() -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .removal(.softDelete()))
    }
    
    /// Retries the task, executing it again.
    /// - Parameters:
    ///  - maxAttempts: The maximum number of attempts that should be made to execute the task. If `nil`, the task will be retried indefinitely.
    /// - action: The action that should be taken when the task fails to execute after the maximum number of attempts has been reached.
    /// - Returns: A `TaskExecutionFailureAction` that will retry the task.
    public static func retry(
        maxAttempts: Int?,
        action: TaskRemovalAction = .dequeue()
    ) -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .retry(maxAttempts: maxAttempts, action: action))
    }
    
    /// Retries the task after a specified interval, executing it again.
    /// - Parameters:
    /// - interval: The interval after which the task should be retried.
    /// - maxAttempts: The maximum number of attempts that should be made to execute the task. If `nil`, the task will be retried indefinitely.
    /// - action: The action that should be taken when the task fails to execute after the maximum number of attempts has been reached.
    /// - Returns: A `TaskExecutionFailureAction` that will retry the task.
    public static func retryAfter(
        _ interval: TimeInterval,
        maxAttempts: Int?,
        action: TaskRemovalAction = .dequeue()
    ) -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .retryAfter(interval, maxAttempts: maxAttempts, action: action))
    }
}

enum TaskExecutionResult {
    case noneExecuted
    case taskSuccessful
    case taskFailure(Error)
}
