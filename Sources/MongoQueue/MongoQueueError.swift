import Foundation
import MongoKitten

/// Errors that can occur when using MongoQueue to manage tasks.
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
    case requeueRecurringTaskFailed
}

public struct QueuedTaskFailure<Context> {
    /// The context that was used to execute the task.
    public let executionContext: Context

    /// The error that occurred when executing the task.
    public let error: Error

    /// The number of attempts that have been made to execute the task.
    public let attemptsMade: Int

    /// The _id of the task that failed.
    public let taskId: ObjectId
}
