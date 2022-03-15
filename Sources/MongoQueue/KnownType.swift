import Logging
import MongoCore
import Foundation
import Meow

internal struct KnownType {
    let category: String
    let performTask: (inout TaskModel) async throws -> ()
    
    init<T: _QueuedTask>(
        type: T.Type,
        queue: MongoQueue,
        logger: Logger,
        context: T.ExecutionContext
    ) {
        self.category = type.category
        self.performTask = { task in
            try await KnownType.performTask(
                &task,
                queue: queue,
                logger: logger,
                ofType: type,
                context: context
            )
        }
    }
    
    private static func performTask<T: _QueuedTask>(
        _ task: inout TaskModel,
        queue: MongoQueue,
        logger: Logger,
        ofType type: T.Type,
        context: T.ExecutionContext
    ) async throws {
        let collection = queue.collection
        var metadata: T
        
        do {
            metadata = try BSONDecoder().decode(type, from: task.metadata)
        } catch {
            logger.error("Task of category \"\(T.category)\" has changed metadata format")
            try await collection.deleteOne(where: "_id" == task._id)
            throw error
        }
        
        let taskConfig = try task.readConfiguration().value
        
        switch taskConfig {
        case .scheduled(let scheduleConfig):
            if let executeBefore = scheduleConfig.executeBefore, executeBefore < Date() {
                logger.info("Task of category \"\(T.category)\" expired and will not be executed")
                do {
                    // TODO: We assume this succeeds, but what if it does not?
                    var concern = WriteConcern()
                    concern.acknowledgement = .majority
                    try await collection.deleteOne(where: "_id" == task._id, writeConcern: concern)
                } catch {
                    logger.critical("Failed to delete task \(task._id) of category \"\(T.category))\" after execution: \(error.localizedDescription)")
                }
                return
            }
        case .recurring(let recurringConfig):
            // No filters exist (yet) that prevent a task from executing
            if let deadline = recurringConfig.deadline, recurringConfig.scheduledDate.addingTimeInterval(deadline) < Date() {
                logger.info("Task of category \"\(T.category)\" expired and will not be executed")
                do {
                    // TODO: We assume this succeeds, but what if it does not?
                    var concern = WriteConcern()
                    concern.acknowledgement = .majority
                    try await collection.deleteOne(where: "_id" == task._id, writeConcern: concern)
                } catch {
                    logger.critical("Failed to delete task \(task._id) of category \"\(T.category))\" after execution: \(error.localizedDescription)")
                }
                return
            }
        }
        
        do {
            task.attempts += 1
            let taskId = task._id
            assert(task.maxTaskDuration >= 30, "maxTaskDuration is set unreasonably low in category \(task.category): \(task.maxTaskDuration)")
            
            // We're early on the updates, so that we don't get dequeued
            let interval = Swift.max(task.maxTaskDuration - 15, 1)
            let executionUpdates = collection.nio.eventLoop.scheduleRepeatedAsyncTask(
                initialDelay: .seconds(Int64(interval)),
                delay: .seconds(Int64(interval)),
                notifying: nil
            ) { executionUpdates in
                collection.nio.findOneAndUpdate(
                    where: "_id" == taskId  ,
                    to: [
                        "$set": [
                            "execution.lastUpdate": Date()
                        ]
                    ]
                ).execute().map { _ in }
            }
            
            defer { executionUpdates.cancel() }
            try await metadata.execute(withContext: context)
            
            try await metadata.onDequeueTask(task, withContext: context, inQueue: queue)
        } catch {
            logger.error("Execution failure for task \(task._id) in category \"\(T.category))\": \(error.localizedDescription)")
            let failureContext = QueuedTaskFailure(
                executionContext: context,
                error: error,
                attemptsMade: task.attempts,
                taskId: task._id
            )
            let onFailure = try await metadata.onExecutionFailure(failureContext: failureContext)
            
            switch onFailure.raw {
            case .dequeue:
                guard try await collection.deleteOne(where: "_id" == task._id).deletes == 1 else {
                    throw MongoQueueError.dequeueTaskFailed
                }
            case .retry(maxAttempts: let maxAttempts):
                if let maxAttempts = maxAttempts, task.attempts >= maxAttempts {
                    guard try await collection.deleteOne(where: "_id" == task._id).deletes == 1 else {
                        throw MongoQueueError.dequeueTaskFailed
                    }
                } else {
                    task.status = .scheduled
                    task.execution = nil
                    
                    guard try await collection.upsertEncoded(task, where: "_id" == task._id).updatedCount == 1 else {
                        throw MongoQueueError.reschedulingFailedTaskFailed
                    }
                }
            case .retryAfter(let nextInterval, maxAttempts: let maxAttempts):
                if let maxAttempts = maxAttempts, task.attempts >= maxAttempts {
                    try await collection.deleteOne(where: "_id" == task._id)
                } else {
                    task.status = .scheduled
                    task.execution = nil
                    task.executeAfter = Date().addingTimeInterval(nextInterval)
                }
            }
            
            // Throw the initial error
            throw error
        }
    }
}
