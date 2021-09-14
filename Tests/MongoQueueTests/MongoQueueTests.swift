import XCTest
import MongoKitten
@testable import MongoQueue

final class MongoQueueTests: XCTestCase {
    func testExample() async throws {
        let db = try MongoDatabase.synchronousConnect("mongodb://localhost/queues")
        let queue = MongoQueue(collection: db["tasks"])
        queue.registerTask(_Task.self, context: ())
        try await queue.queueTask(_Task(message: "Test0"))
        try queue.runInBackground()
        try await queue.queueTask(_Task(message: "Test1"))
        try await queue.queueTask(_Task(message: "Test2"))
        try await queue.queueTask(_Task(message: "Test3"))
        
        // Sleep 5 sec
        await Task.sleep(5_000_000_000)
    }
}

struct _Task: ScheduledTask {
    var scheduledDate: Date {
        Date()
    }
    
    let message: String
    
    func execute(withContext context: Void) async throws {
        print(message)
    }
    
    func onExecutionFailure(failureContext: QueuedTaskFailure<()>) async throws -> TaskExecutionFailureAction {
        return .dequeue()
    }
}
