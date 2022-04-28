import Foundation

public struct TaskRemovalAction {
    enum _Raw {
        case dequeue, softDelete
    }
    
    let raw: _Raw
    
    public static func dequeue() -> TaskRemovalAction {
        TaskRemovalAction(raw: .dequeue)
    }
    
    public static func softDelete() -> TaskRemovalAction {
        TaskRemovalAction(raw: .softDelete)
    }
}

public struct TaskExecutionFailureAction {
    enum _Raw {
        case removal(TaskRemovalAction)
        case retry(maxAttempts: Int?, action: TaskRemovalAction)
        case retryAfter(TimeInterval, maxAttempts: Int?, action: TaskRemovalAction)
    }
    
    let raw: _Raw
    
    public static func dequeue() -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .removal(.dequeue()))
    }
    
    public static func softDelete() -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .removal(.softDelete()))
    }
    
    public static func retry(
        maxAttempts: Int?,
        action: TaskRemovalAction = .dequeue()
    ) -> TaskExecutionFailureAction {
        TaskExecutionFailureAction(raw: .retry(maxAttempts: maxAttempts, action: action))
    }
    
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
