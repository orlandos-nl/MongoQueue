import Logging
import MongoCore
import Foundation
import Meow

/// A protocol that describes a task that can be executed on a recurring basis (e.g. every day, every month, etc)
///
/// When conforming to this type, you're also conforming to `Codable`. Using this Codable conformance, all stored properties will be stored in and retrieved from MongoDB. Your task's `execute` function represents your business logic of how tasks are handled, whereas the stored properties of this type represent the input you to execute this work.
///
/// The context provided into ``execute`` can be any type of your choosing, and is used as a means to execute the task. In the case of an newsletter task, this would be the email client.
///
/// ```swift
/// struct DailyReminder: RecurringTask {
///   typealias ExecutionContext = SMTPClient
///
///   // Stored properties are encoded to MongoDB
///   // When the task runs, they'll be decoded into a new `Reminder` instance
///   // After which `execute` will be called
///   let username: String
///
///   // A mandatory property, allowing MongoQueue to set the initial execution date
///   // In this case, MongoQueue will set the execution date to "now".
///   // This causes the task to be ran as soon as possible.
///   // Because this is computed, the property is not stored in MongoDB.
///   var initialTaskExecutionDate: Date { Date() }
///
///   // Your business logic for this task, which can `mutate` self.
///   // This allow it to pass updated info into the next iteration.
///   mutating func execute(withContext: context: ExecutionContext) async throws {
///     print("I'm running! Wake up, \(username)")
///   }
///
///   // Calculate the next time when this task should be executed again
///   func getNextRecurringTaskDate(_ context: ExecutionContext) async throws -> Date? {
///     // Re-run again in 24 hours
///     return Date().addingTimeInterval(3600 * 24)
///   }
///
///   // What to do when `execute` throws an error
///   func onExecutionFailure(
///     failureContext: QueuedTaskFailure<MyTaskContext>
///   ) async throws -> TaskExecutionFailureAction {
///      // Removes the task from the queue without re-attempting
///      return .dequeue()
///   }
/// }
/// ```
public protocol RecurringTask: _QueuedTask {
    /// The moment that you want this to be first executed on (delay)
    /// If you want it to be immediate, use `Date()`
    var initialTaskExecutionDate: Date { get }

    /// Tasks won't be executed after this moment
    var taskExecutionDeadline: TimeInterval? { get }
    
    /// Calculates the next moment that this task should be executed on (e.g. next month, next day, etc).
    /// This is called _after_ your `execute` function has successfully completed the work.
    /// If you want to stop recurring, return `nil`.
    /// - parameter context: The context that was used to execute the task.
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
    /// The deadline for this task to be executed on. After this deadline, the task will not be executed, even if it is still in the queue.
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
    
    /// The configuration for this task. This is used to identify the task within the queue, for internal use.
    public var configuration: _TaskConfiguration {
        let recurring = RecurringTaskConfiguration(
            scheduledDate: initialTaskExecutionDate,
            uniqueTaskKey: uniqueTaskKey,
            deadline: taskExecutionDeadline
        )
        return _TaskConfiguration(value: .recurring(recurring))
    }
}
