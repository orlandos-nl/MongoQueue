import Foundation
import MongoKitten

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
    public let executionContext: Context
    public let error: Error
    public let attemptsMade: Int
    public let taskId: ObjectId
}
