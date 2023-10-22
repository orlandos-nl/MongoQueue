import XCTest
import MongoKitten
@testable import MongoQueue

final class MongoQueueTests: XCTestCase {
    static var ranTasks = 0
    let settings = try! ConnectionSettings("mongodb://\(ProcessInfo.processInfo.environment["MONGO_HOSTNAME_A"] ?? "localhost")/queues")
    
    func testExample() async throws {
        Self.ranTasks = 0
        let db = try await MongoDatabase.connect(to: settings)
        try await db.drop()
        let queue = MongoQueue(collection: db["tasks"])
        queue.registerTask(_Task.self, context: ())
        try await queue.queueTask(_Task(message: 0))
        try queue.runInBackground()
        try await queue.queueTask(_Task(message: 1))
        try await queue.queueTask(_Task(message: 2))
        try await queue.queueTask(_Task(message: 3))
        
        // Sleep 10 sec
        try await Task.sleep(nanoseconds: 10_000_000_000)
        
        XCTAssertEqual(Self.ranTasks, 4)
    }

    @available(macOS 13.0, *)
    func testMaxParallelJobs() async throws {
        let db = try await MongoDatabase.connect(to: settings)
        try await db.drop()
        let queue = MongoQueue(collection: db["tasks"])
        queue.setMaxParallelJobs(to: 6)
        queue.registerTask(SlowTask.self, context: ())

        let start = Date()

        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())

        try await queue.runUntilEmpty()

        XCTAssertLessThanOrEqual(-start.timeIntervalSinceNow, 2)

        XCTAssertEqual(Self.ranTasks, 6)
    }

    @available(macOS 13.0, *)
    func testMaxParallelJobsLow() async throws {
        let db = try await MongoDatabase.connect(to: settings)
        try await db.drop()
        let queue = MongoQueue(collection: db["tasks"])
        queue.setMaxParallelJobs(to: 1)
        queue.registerTask(SlowTask.self, context: ())

        let start = Date()

        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())
        try await queue.queueTask(SlowTask())

        try await queue.runUntilEmpty()

        XCTAssertGreaterThanOrEqual(-start.timeIntervalSinceNow, 6)

        XCTAssertEqual(Self.ranTasks, 6)
    }

    func testNoDuplicateQueuedTasksOfSameUniqueKey() async throws {
        struct UniqueTask: ScheduledTask {
            var taskExecutionDate: Date { Date() }
            var uniqueTaskKey: String { "static" }

            func execute(withContext context: Void) async throws {}

            func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
                return .dequeue()
            }
        }

        let db = try await MongoDatabase.connect(to: settings)
        try await db.drop()
        let queue = MongoQueue(collection: db["tasks"], options: [.enableUniqueKeys])
        queue.registerTask(UniqueTask.self, context: ())
        try await queue.ensureIndexes()
        try await queue.queueTask(UniqueTask())

        do {
            try await queue.queueTask(UniqueTask())
            XCTFail("Task should not be able to exist in queue twice")
        } catch {}

        try await queue.runUntilEmpty()
        try await queue.queueTask(UniqueTask())
    }

    func testDuplicatedOfDifferentTasksCanExist() async throws {
        struct UniqueTask: ScheduledTask {
            var taskExecutionDate: Date { Date() }
            var uniqueTaskKey: String { "static" }

            func execute(withContext context: Void) async throws {}

            func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
                return .dequeue()
            }
        }

        struct UniqueTask2: ScheduledTask {
            var taskExecutionDate: Date { Date() }
            var uniqueTaskKey: String { "static" }

            func execute(withContext context: Void) async throws {}

            func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
                return .dequeue()
            }
        }

        let db = try await MongoDatabase.connect(to: settings)
        try await db.drop()
        let queue = MongoQueue(collection: db["tasks"], options: [.enableUniqueKeys])
        queue.registerTask(UniqueTask.self, context: ())
        queue.registerTask(UniqueTask2.self, context: ())
        try await queue.ensureIndexes()
        try await queue.queueTask(UniqueTask())
        try await queue.queueTask(UniqueTask2())

        do {
            try await queue.queueTask(UniqueTask())
            XCTFail("Task should not be able to exist in queue twice")
        } catch {}

        do {
            try await queue.queueTask(UniqueTask2())
            XCTFail("Task should not be able to exist in queue twice")
        } catch {}

        try await queue.runUntilEmpty()
        try await queue.queueTask(UniqueTask())
        try await queue.queueTask(UniqueTask2())
    }

    func test_recurringTask() async throws {
        Self.ranTasks = 0
        let db = try await MongoDatabase.connect(to: settings)
        try await db.drop()
        let queue = MongoQueue(collection: db["tasks"])
        queue.registerTask(RTRecurringTask.self, context: ())
        try await queue.queueTask(RTRecurringTask())
        queue.newTaskPollingFrequency = .milliseconds(100)
        try queue.runInBackground()
        
        // Sleep 30 sec, so each 5-second window is ran, +5 seconds to test if it runs only 5 times
        try await Task.sleep(nanoseconds: 5_000_000_000)
        
        XCTAssertEqual(Self.ranTasks, 5)
    }
}

struct _Task: ScheduledTask {
    var taskExecutionDate: Date {
        Date()
    }

    let message: Int

    func execute(withContext context: Void) async throws {
        XCTAssertEqual(MongoQueueTests.ranTasks, message)
        MongoQueueTests.ranTasks += 1
    }

    func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
        return .dequeue()
    }
}

@available(macOS 13.0, *)
struct SlowTask: ScheduledTask {
    var taskExecutionDate: Date {
        Date()
    }

    func execute(withContext context: Void) async throws {
        try await Task.sleep(for: .seconds(1))
        MongoQueueTests.ranTasks += 1
    }

    func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
        return .dequeue()
    }
}

struct RTRecurringTask: RecurringTask {
    typealias ExecutionContext = Void
    var initialTaskExecutionDate: Date { Date() }
    
    var uniqueTaskKey: String = "RecurringTask"
    
    func getNextRecurringTaskDate(_ context: ExecutionContext) async throws -> Date? {
        MongoQueueTests.ranTasks >= 5 ? nil : Date().addingTimeInterval(1)
    }
        
    func execute(withContext context: Void) async throws {
        MongoQueueTests.ranTasks += 1
    }
    
    func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
        return .dequeue()
    }
}
