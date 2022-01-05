import Logging
import MongoCore
import Foundation
import Meow

public protocol ScheduledTask: _QueuedTask {
    /// The date that you want this to be executed (delay)
    /// If you want it to be immediate, use `Date()`
    var taskExecutionDate: Date { get }
    
    /// Tasks won't be executed after this moment
    var taskExecutionDeadline: Date? { get }
}

extension ScheduledTask {
    public var taskExecutionDeadline: Date? { nil }
    
    public func onDequeueTask(_ task: TaskModel, withContext context: ExecutionContext, inQueue queue: MongoQueue) async throws {
        do {
            // TODO: We assume this succeeds, but what if it does not?
            var concern = WriteConcern()
            concern.acknowledgement = .majority
            
            guard try await queue.collection.deleteOne(where: "_id" == task._id, writeConcern: concern).deletes == 1 else {
                throw MongoQueueError.dequeueTaskFailed
            }
        } catch {
            queue.logger.critical("Failed to delete task \(task._id) of category \"\(Self.category))\" after execution: \(error.localizedDescription)")
        }
    }
    
    public var configuration: _TaskConfiguration {
        let scheduled = ScheduledTaskConfiguration(
            scheduledDate: taskExecutionDate,
            executeBefore: taskExecutionDeadline
        )
        return _TaskConfiguration(value: .scheduled(scheduled))
    }
}

public struct ScheduledTaskConfiguration: Codable {
    let scheduledDate: Date
    let executeBefore: Date?
}
