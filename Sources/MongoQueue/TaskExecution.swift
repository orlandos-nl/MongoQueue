import Foundation

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

enum TaskExecutionResult {
    case noneExecuted
    case taskSuccessful
    case taskFailure(Error)
}
