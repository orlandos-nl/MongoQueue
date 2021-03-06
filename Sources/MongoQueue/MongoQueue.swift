import Logging
import MongoCore
import Foundation
import Meow

// TODO: ReadConcern majority in >= MongoDB 4.2
public final class MongoQueue {
    internal let collection: MongoCollection
    internal let logger = Logger(label: "org.openkitten.mongo-queues")
    private var knownTypes = [KnownType]()
    private var started = false
    private var serverHasData = true
    private var task: Task<Void, Never>?
    public var newTaskPollingFrequency = NIO.TimeAmount.milliseconds(1000)
    public var stalledTaskPollingFrequency = NIO.TimeAmount.seconds(30)
//    private let executingTasks = ExecutingTasks()
    
    public init(collection: MongoCollection) {
        self.collection = collection
    }
    
    public func registerTask<T: _QueuedTask>(
        _ type: T.Type,
        context: T.ExecutionContext
    ) {
        knownTypes.append(KnownType(type: type, queue: self, logger: logger, context: context))
    }
    
    @discardableResult
    public func suspendTasks<T: _QueuedTask>(ofType type: T.Type, where filter: Document = [:]) async throws -> UpdateReply {
        try await collection.updateMany(
            where: [
                "status": TaskStatus.scheduled.raw.rawValue,
                "metadata": filter
            ],
             setting: [
                "status": TaskStatus.suspended.raw.rawValue
             ],
             unsetting: nil
        )
    }
    
    @discardableResult
    public func unsuspendTasks<T: _QueuedTask>(ofType type: T.Type, where filter: Document = [:]) async throws -> UpdateReply {
        try await collection.updateMany(
            where: [
                "status": TaskStatus.suspended.raw.rawValue,
                "metadata": filter
            ],
             setting: [
                "status": TaskStatus.scheduled.raw.rawValue
             ],
             unsetting: nil
        )
    }
    
    @discardableResult
    public func suspendTasks<T: _QueuedTask, Q: MongoKittenQuery>(ofType type: T.Type, where filter: Q) async throws -> UpdateReply {
        try await suspendTasks(ofType: type, where: filter.makeDocument())
    }
    
    @discardableResult
    public func unsuspendTasks<T: _QueuedTask, Q: MongoKittenQuery>(ofType type: T.Type, where filter: Q) async throws -> UpdateReply {
        try await unsuspendTasks(ofType: type, where: filter.makeDocument())
    }
    
    func runNextTask() async throws -> TaskExecutionResult {
        let context = try BSONEncoder().encode(TaskModel.ExecutingContext())
        var writeConcern = WriteConcern()
        writeConcern.acknowledgement = .majority
        
        var filter: Document = "status" == TaskStatus.scheduled.raw.rawValue
        let executeAfterFilter: Document = "executeAfter" <= Date()
        filter = (filter && executeAfterFilter).makeDocument()
        
        let reply = try await collection.findOneAndUpdate(
            where: filter,
            to: [
                "$set": [
                    "status": TaskStatus.executing.raw.rawValue,
                    "execution": context
                ] as Document
            ],
            returnValue: .modified
        )
            .sort([
                "priority": .descending,
                "executeBefore": .ascending,
                "creationDate": .ascending
            ])
            .writeConcern(writeConcern)
            .execute()
        
        guard let taskDocument = reply.value else {
            // No task found
            return .noneExecuted
        }
        
        var task = try BSONDecoder().decode(TaskModel.self, from: taskDocument)
        guard let knownType = knownTypes.first(where: { $0.category == task.category }) else {
            throw MongoQueueError.unknownTaskCategory
        }
        
        do {
            try await knownType.performTask(&task)
            return .taskSuccessful
        } catch {
            return .taskFailure(error)
        }
    }
    
    @discardableResult
    public func runInBackground() throws -> Task<Void, Error> {
        if started {
            throw MongoQueueError.alreadyStarted
        }
        
        return Task {
            try await self.run()
        }
    }
    
    public func run() async throws {
        if started {
            throw MongoQueueError.alreadyStarted
        }
        
        started = true
        
        // If another instance crashed, causing a stale task, this ensures the task gets requeued
        task = Task { [weak self] in
            repeat {
                guard let queue = self else {
                    return
                }
                
                _ = try? await queue.findAndRequeueStaleTasks()
                _ = try? await Task.sleep(nanoseconds: UInt64(stalledTaskPollingFrequency.nanoseconds))
            } while !Task.isCancelled
        }
        
        if let wireVersion = await collection.database.pool.wireVersion, wireVersion.supportsCollectionChangeStream {
            try await cursorInitiatedTick()
            try await self.startChangeStreamTicks()
        } else {
            try await self.sleepBasedTick()
        }
    }
    
    public func shutdown() {
        self.started = false
    }
    
    private func startChangeStreamTicks() async throws {
        // Using change stream cursor based polling
        var options = ChangeStreamOptions()
        options.maxAwaitTimeMS = 50
        var cursor = try await collection.watch(options: options, type: TaskModel.self)
        cursor.setGetMoreInterval(to: newTaskPollingFrequency)
        let iterator = cursor.forEach { change in
            if change.operationType == .insert || change.operationType == .update || change.operationType == .replace {
                // Dataset changed, retry
                if !self.serverHasData {
                    self.serverHasData = true
                    Task {
                        try await self.cursorInitiatedTick()
                    }
                }
            }
            
            return self.started
        }
        
        // Kick off the first tick, because we might immediately have work
        try await self.cursorInitiatedTick()
        
        // The change stream only observes _changes_ to datasets
        // That allows us to respond to new tasks rapidly
        // We still want background ticks so that a 
        let backgroundTicks = Task {
            try await self.sleepBasedTick()
        }
        
        do {
            // Ideally, this is where we stay
            try await iterator.value
        } catch {}
        
        backgroundTicks.cancel()
        
        if self.started {
            // Restart Change Stream
            try await startChangeStreamTicks()
        }
    }
    
    private func cursorInitiatedTick() async throws {
        do {
            switch try await self.runNextTask() {
            case .taskSuccessful, .taskFailure:
                Task {
                    try await self.cursorInitiatedTick()
                }
            case .noneExecuted:
                serverHasData = false
            }
        } catch {
            // Task execution failed due to a MongoDB error
            // Otherwise the return type would specify the task status
            serverHasData = false
        }
    }
    
    private func sleepBasedTick() async throws {
        while started {
            if !serverHasData {
                try await Task.sleep(nanoseconds: UInt64(newTaskPollingFrequency.nanoseconds))
            }
            
            do {
                if case .noneExecuted = try await self.runNextTask() {
                    serverHasData = false
                } else {
                    serverHasData = true
                }
            } catch {
                // Task execution failed due to a MongoDB error
                // Otherwise the return type would specify the task status
                serverHasData = false
            }
        }
    }
    
    public func queueTask<T: _QueuedTask>(_ task: T) async throws {
        assert(knownTypes.contains(where: { $0.category == T.category }))
        
        let model = try TaskModel(representing: task)
        
        var writeConcern = WriteConcern()
        writeConcern.acknowledgement = .majority
        
        let reply = try await collection.insertEncoded(model, writeConcern: writeConcern)
        guard reply.insertCount == 1 else {
            throw MongoQueueError.taskCreationFailed
        }
    }
    
    private func findAndRequeueStaleTasks() async throws {
        for type in knownTypes {
            let executingTasks = try await collection.find(
                "category" == type.category && "status" == TaskStatus.executing.raw.rawValue
            ).decode(TaskModel.self).execute()
            
            for try await task in executingTasks {
                if
                    let lastUpdateDate = task.execution?.lastUpdate,
                    lastUpdateDate.addingTimeInterval(task.maxTaskDuration) <= Date()
                {
                    logger.info("Dequeueing stale task id \(task._id) of type \(task.category)")
                    _ = try await collection.findOneAndUpdate(where: "_id" == task._id, to: [
                        "$set": [
                            "status": TaskStatus.scheduled.raw.rawValue,
                            "execution": Null()
                        ] as Document
                    ]).execute()
                }
            }
        }
    }
}
