import Logging
import MongoCore
import Foundation
import Meow

public protocol RecurringTask: _QueuedTask {
    /// The moment that you want this to be executed on (delay)
    /// If you want it to be immediate, use `Date()`
    var initialTaskExecutionDate: Date { get }
    
    /// If you want only one task of this type to exist, use a static task key
    /// If you want to have many tasks, but not duplicate the task, identify this task by the task key
    /// If you don't want this task to be uniquely identified, and you want to spawn many of them, use `UUID().uuidString`
    var uniqueTaskKey: String { get }
    var taskExecutionDeadline: TimeInterval? { get }
    
    func getNextRecurringTaskDate(_ context: ExecutionContext) async throws -> Date?
}

struct ScheduledInterval: Codable {
    private(set) var nextOccurrance: Date
    let schedule: Schedule
    
    enum Schedule: Codable {
        case monthly//(..)
        case daily//(..)
        
        func nextMoment(from date: Date = Date()) -> Date {
            fatalError()
        }
    }
    
    init(schedule: Schedule) {
        self.nextOccurrance = schedule.nextMoment()
        self.schedule = schedule
    }
    
    mutating func increment() {
        nextOccurrance = schedule.nextMoment(from: nextOccurrance)
    }
}

extension RecurringTask {
    public var taskExecutionDeadline: TimeInterval? { nil }
    
    public func _onDequeueTask(_ task: TaskModel, withContext context: ExecutionContext, inQueue queue: MongoQueue) async throws -> _DequeueResult {
        do {
            guard case .recurring(let taskConfig) = try task.readConfiguration().value else {
                assertionFailure("Invalid internal MongoQueue state")
                return _DequeueResult()
            }
            var concern = WriteConcern()
            concern.acknowledgement = .majority
            if let nextDate = try await getNextRecurringTaskDate(context) {
                var task = task
                task.metadata = try BSONEncoder().encode(self)
                task.execution = nil
                task.status = .scheduled
                task.executeAfter = nextDate
                task.executeBefore = taskConfig.deadline.map { deadline in
                    task.executeAfter.addingTimeInterval(deadline)
                }
                
                // TODO: We assume this succeeds, but what if it does not?
                // TODO: WriteConcern majority
                guard try await queue.collection.upsertEncoded(task, where: "_id" == task._id).updatedCount == 1 else {
                    throw MongoQueueError.requeueRecurringTaskFailed
                }
            } else {
                // TODO: We assume this succeeds, but what if it does not?
                guard try await queue.collection.deleteOne(where: "_id" == task._id, writeConcern: concern).deletes == 1 else {
                    throw MongoQueueError.dequeueTaskFailed
                }
            }
        } catch {
            queue.logger.critical("Failed to delete task \(task._id) of category \"\(Self.category))\" after execution: \(error.localizedDescription)")
        }
        
        return _DequeueResult()
    }
    
    public var configuration: _TaskConfiguration {
        let recurring = RecurringTaskConfiguration(
            scheduledDate: initialTaskExecutionDate,
            key: uniqueTaskKey,
            deadline: taskExecutionDeadline
        )
        return _TaskConfiguration(value: .recurring(recurring))
    }
}
