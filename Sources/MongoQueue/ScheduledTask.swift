import Logging
import MongoCore
import Foundation
import Meow

/// A task that is scheduled to be executed at a specific moment in time.
/// This task will be executed once, and then removed from the queue.
///
/// When conforming to this type, you're also conforming to `Codable`. Using this Codable conformance, all stored properties will be stored in and retrieved from MongoDB. Your task's `execute` function represents your business logic of how tasks are handled, whereas the stored properties of this type represent the input you to execute this work.
///
/// The context provided into ``execute`` can be any type of your choosing, and is used as a means to execute the task. In the case of an newsletter task, this would be the email client.
///
/// ```swift
/// struct Reminder: ScheduledTask {
///   typealias ExecutionContext = SMTPClient
///
///   // Stored properties are encoded to MongoDB
///   // When the task runs, they'll be decodd into a new `Reminder` instance
///   // After which `execute` will be called
///   let username: String
///
///   // A mandatory property, allowing MongoQueue to set the execution date
///   // In this case, MongoQueue will set the execution date to "now".
///   // This causes the task to be ran as soon as possible.
///   // Because this is computed, the property is not stored in MongoDB.
///   var taskExecutionDate: Date { Date() }
///
///   // Your business logic for this task
///   func execute(withContext: context: ExecutionContext) async throws {
///     print("I'm running! Hello, \(username)")
///   }
///
///   // What to do when `execute` throws an error
///   func onExecutionFailure(
///     failureContext: QueuedTaskFailure<ExecutionContext>
///   ) async throws -> TaskExecutionFailureAction {
///      // Removes the task from the queue without re-attempting
///      return .dequeue()
///   }
/// }
/// ```
public protocol ScheduledTask: _QueuedTask {
    /// The date that you want this to be executed (delay)
    /// If you want it to be immediate, use `Date()`
    var taskExecutionDate: Date { get }
    
    /// Tasks won't be executed after this moment
    var taskExecutionDeadline: Date? { get }
    
    /// What happens when this task completes successfully
    var taskRemovalAction: TaskRemovalAction { get }
}

extension ScheduledTask {
    public var taskExecutionDeadline: Date? { nil }
    public var taskRemovalAction: TaskRemovalAction { .dequeue() }
    
    public func _onDequeueTask(_ task: TaskModel, withContext context: ExecutionContext, inQueue queue: MongoQueue) async throws -> _DequeueResult {
        do {
            // TODO: We assume this succeeds, but what if it does not?
            var concern = WriteConcern()
            concern.acknowledgement = .majority
            
            switch taskRemovalAction.raw {
            case .dequeue:
                guard try await queue.collection.deleteOne(where: "_id" == task._id, writeConcern: concern).deletes == 1 else {
                    throw MongoQueueError.dequeueTaskFailed
                }
            case .softDelete:
                var task = task
                task.status = .dequeued
                task.execution?.lastUpdate = Date()
                task.execution?.endState = .success
                
                let update = try await queue.collection.upsertEncoded(task, where: "_id" == task._id)
                
                guard update.updatedCount == 1 else {
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
        let scheduled = ScheduledTaskConfiguration(
            scheduledDate: taskExecutionDate,
            uniqueTaskKey: uniqueTaskKey,
            executeBefore: taskExecutionDeadline
        )
        return _TaskConfiguration(value: .scheduled(scheduled))
    }
}
