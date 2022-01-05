import XCTest
import MongoKitten
@testable import MongoQueue

final class MongoQueueTests: XCTestCase {
    static var ranTasks = 0
    
    func testExample() async throws {
        Self.ranTasks = 0
        let db = try MongoDatabase.synchronousConnect("mongodb://localhost/queues")
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
    
    func test_recurringTask() async throws {
        Self.ranTasks = 0
        let db = try MongoDatabase.synchronousConnect("mongodb://localhost/queues")
        let queue = MongoQueue(collection: db["tasks"])
        queue.registerTask(RTRecurringTask.self, context: ())
        try await queue.queueTask(RTRecurringTask())
        try queue.runInBackground()
        
        // Sleep 15 sec
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

struct RTRecurringTask: RecurringTask {
    typealias ExecutionContext = Void
    var initialTaskExecutionDate: Date { Date() }
    
    var uniqueTaskKey: String = "RecurringTask"
    
    func getNextRecurringTaskDate(_ context: ExecutionContext) async throws -> Date? {
        MongoQueueTests.ranTasks >= 5 ? nil : Date()
    }
        
    func execute(withContext context: Void) async throws {
        MongoQueueTests.ranTasks += 1
    }
    
    func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
        return .dequeue()
    }
}
