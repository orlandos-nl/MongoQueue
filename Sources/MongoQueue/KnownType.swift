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
        logger.debug("Executing task \(task._id) of category \"\(T.category)\"")
        let collection = queue.collection
        var metadata: T
        
        do {
            metadata = try BSONDecoder().decode(type, from: task.metadata)
        } catch {
            logger.error("Task of category \"\(T.category)\" has changed metadata format")
            queue.jobsInvalid.increment()
            try await collection.deleteOne(where: "_id" == task._id)
            queue.jobsRemoved.increment()
            throw error
        }
        
        let taskConfig = try task.readConfiguration().value
        
        switch taskConfig {
        case .scheduled(let scheduleConfig):
            if let executeBefore = scheduleConfig.executeBefore, executeBefore < Date() {
                queue.jobsExpired.increment()

                logger.info("Task of category \"\(T.category)\" expired and will not be executed")
                do {
                    // TODO: We assume this succeeds, but what if it does not?
                    var concern = WriteConcern()
                    concern.acknowledgement = .majority
                    try await collection.deleteOne(where: "_id" == task._id, writeConcern: concern)
                    queue.jobsRemoved.increment()
                } catch {
                    logger.critical("Failed to delete task \(task._id) of category \"\(T.category))\" after execution: \(error.localizedDescription)")
                }
                return
            }
        case .recurring(let recurringConfig):
            // No filters exist (yet) that prevent a task from executing
            if let deadline = recurringConfig.deadline, recurringConfig.scheduledDate.addingTimeInterval(deadline) < Date() {
                queue.jobsExpired.increment()

                logger.info("Task of category \"\(T.category)\" expired and will not be executed")
                do {
                    // TODO: We assume this succeeds, but what if it does not?
                    var concern = WriteConcern()
                    concern.acknowledgement = .majority
                    try await collection.deleteOne(where: "_id" == task._id, writeConcern: concern)
                    queue.jobsRemoved.increment()
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
            try await withThrowingTaskGroup(of: T.self) { taskGroup in
                taskGroup.addTask {
                    while !Task.isCancelled {
                        try await Task.sleep(nanoseconds: UInt64(interval) * 1_000_000_000)
                        _ = try await collection.findOneAndUpdate(
                            where: "_id" == taskId  ,
                            to: [
                                "$set": [
                                    "execution.lastUpdate": Date()
                                ]
                            ]
                        ).execute()
                    }

                    throw CancellationError()
                }

                taskGroup.addTask { [metadata, task] in
                    var metadata = metadata
                    queue.jobsRan.increment()
                    try await metadata.execute(withContext: context)
                    queue.jobsSucceeded.increment()
                    logger.debug("Successful execution: task \(task._id) of category \"\(T.category)\"")
                    _ = try await metadata._onDequeueTask(task, withContext: context, inQueue: queue)
                    return metadata
                }

                guard let _metadata = try await taskGroup.next() else {
                    throw CancellationError()
                }

                metadata = _metadata
                taskGroup.cancelAll()
            }
        } catch {
            queue.jobsFailed.increment()
            logger.debug("Execution failure for task \(task._id) in category \"\(T.category))\": \(error.localizedDescription)")
            let failureContext = QueuedTaskFailure(
                executionContext: context,
                error: error,
                attemptsMade: task.attempts,
                taskId: task._id
            )
            let onFailure = try await metadata.onExecutionFailure(failureContext: failureContext)
            
            func applyRemoval(_ removal: TaskRemovalAction) async throws {
                switch removal.raw {
                case .softDelete:
                    task.status = .dequeued
                    task.execution?.lastUpdate = Date()
                    task.execution?.endState = .failure
                    
                    let update = try await queue.collection.upsertEncoded(task, where: "_id" == task._id)
                    
                    guard update.updatedCount == 1 else {
                        logger.error("Failed to soft-delete task \(task._id) of category \"\(T.category)\"")
                        throw MongoQueueError.dequeueTaskFailed
                    }
                case .dequeue:
                    guard try await collection.deleteOne(where: "_id" == task._id).deletes == 1 else {
                        logger.error("Failed to delete task \(task._id) of category \"\(T.category)\"")
                        throw MongoQueueError.dequeueTaskFailed
                    }
                }
                queue.jobsRemoved.increment()
            }
            
            switch onFailure.raw {
            case .removal(let removal):
                try await applyRemoval(removal)
            case .retry(maxAttempts: let maxAttempts, let removal):
                if let maxAttempts = maxAttempts, task.attempts >= maxAttempts {
                    logger.debug("Task Removal: task \(task._id) of category \"\(T.category)\" exceeded \(maxAttempts) attempts")
                    try await applyRemoval(removal)
                } else {
                    task.status = .scheduled
                    task.execution = nil
                    
                    guard try await collection.upsertEncoded(task, where: "_id" == task._id).updatedCount == 1 else {
                        throw MongoQueueError.reschedulingFailedTaskFailed
                    }
                    queue.jobsRequeued.increment()
                }
            case .retryAfter(let nextInterval, maxAttempts: let maxAttempts, let removal):
                if let maxAttempts = maxAttempts, task.attempts >= maxAttempts {
                    logger.debug("Task Removal: task \(task._id) of category \"\(T.category)\" exceeded \(maxAttempts) attempts")
                    try await applyRemoval(removal)
                } else {
                    task.status = .scheduled
                    task.execution = nil
                    task.executeAfter = Date().addingTimeInterval(nextInterval)

                    guard try await collection.upsertEncoded(task, where: "_id" == task._id).updatedCount == 1 else {
                        throw MongoQueueError.reschedulingFailedTaskFailed
                    }
                    queue.jobsRequeued.increment()
                }
            }
            
            // Throw the initial error
            throw error
        }
    }
}
