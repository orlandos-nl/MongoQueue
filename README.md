# <img height="200px" style="float: right;" src="https://user-images.githubusercontent.com/1951674/224335889-c6345081-fef9-4b01-95ba-c3a718aa91e9.png" /> MongoQueue

A [MongoKitten](https://github.com/orlandos-nl/MongoKitten) based JobQueue for MongoDB.

[Join our Discord](https://discord.gg/H6799jh) for any questions and friendly banter.

[Read the Docs](https://swiftpackageindex.com/orlandos-nl/MongoQueue/main/documentation/mongoqueue) for more info.

### Quick Start

Connect to MongoDB with MongoKitten regularly:

```swift
let db = try await MongoDatabase.connect(to: "mongodb://localhost/my_database")
```

Select a collection for your job queue:

```swift
let queue = MongoQueue(collection: db["tasks"])
```

Define your jobs by conforming to `ScheduledTask` (and implicitly `Codable`):

```swift
struct RegistrationEmailTask: ScheduledTask {
    // This type has no context
    typealias ExecutionContext = Void

    // Computed property, required by ScheduledTask
    // This executed the task ASAP
    var taskExecutionDate: Date { Date() }
    
    // Stored properties represent the metadata needed to execute the task
    let recipientEmail: String
    let userId: ObjectId
    let fullName: String
    
    func execute(withContext context: ExecutionContext) async throws {
        // TODO: Send the email
        // Throwing an error triggers `onExecutionFailure`
    }
    
    func onExecutionFailure(failureContext: QueuedTaskFailure<ExecutionContext>) async throws -> TaskExecutionFailureAction {
        // Only attempt the job once. Failing to send the email cancels the job
        return .dequeue()
    }
}
```

Register the task to MongoQueue, before it starts.

```swift
// Context is `Void`, so we pass in a void here
queue.registerTask(RegistrationEmailTask.self, context: ())
```

Start the queue in the background - this is helpful in use inside HTTP applications - or when creating separate workers.

```swift
try queue.runInBackground()
```

Alternatively, run the queue in the foreground and block until the queue is stopped. Only use this if your queue worker is only running as a worker. I.E., it isn't serving users on the side.

```swift
try await queue.run()
```

Queue the task in MongoDB:

```swift
let task = RegistrationEmailTask(
  recipientEmail: "joannis@orlandos.nl",
  userId: ...,
  fullName: "Joannis Orlandos"
)
try await queue.queueTask(task)
```

Tada! Just wait for it to be executed.

## Testing

You can run all currently active jobs in the queue one-by-one using:

```swift
try await queue.runUntilEmpty()
``` 

This will not run any jobs scheduled for the future, and will exit once there are no current jobs available.

## Parallelisation

You can set the parallelisation amount **per job queue instance** using the following code:

```swift
queue.setMaxParallelJobs(to: 6)
```

If you have two containers running an instance of MongoQueue, it will therefore be able to run `2 * 6 = 12` jobs simultaneously.

## Integrate with Vapor

To access the `queue` from your Vapor Request, add the following snippet:

```swift
import Vapor
import MongoKitten
import MongoQueue

extension Request {
  public var queue: MongoQueue {
    return application.queue
  }
}

private struct MongoQueueStorageKey: StorageKey {
  typealias Value = MongoQueue
}

extension Application {
  public var queue: MongoQueue {
    get {
      storage[MongoQueueStorageKey.self]!
    }
    set {
      storage[MongoQueueStorageKey.self] = newValue
    }
  }

  public func initializeMongoQueue(withCollection collection: MongoCollection) {
    self.queue = MongoQueue(collection: collection)
  }
}
```

From here, you can add tasks as such:

```swift
app.post("tasks") { req in
  try await req.queue.queueTask(MyCustomTask())
  return HTTPStatus.created
}
```

# Advanced Use

Before diving into more (detailed) APIs, here's an overview of how this works:

When you queue a task, it is used to derive the basic information for queueing the job. Parts of these requirements are in the protocol, but have a default value provided by MongoQueue.

## Dequeing Process

Each task has a **category**, a unique string identifying this task's type in the database. When you register your task with MongoQueue, the category is used to know how to decode & execute the task once it is acquired by a worker.

MongoQueue regularly checks, on a timer (and if possible with Change Streams for better responsiveness) whether a new task is ready to grab. When it pulls a task from MongoDB, it takes the **highest priority** task that is scheduled for execution at this date.

The priority is `.normal` by default, but urgency can be increased or decreased in a tasks `var priority: TaskPriority { get }`.

When the task is taken out of the queue, its `status` is set to `executing`. This means that other jobs can't execute this task right now. While doing so, the task model's `maxTaskDuration` is used as an indication of the expected duration of a task. The expected deadline is set on the model in MongoDB by adding `maxTaskDuration` to the current date.

If the deadline is reached, other workers can (and will) dequeue the task and put it back into `scheduled`. This assumes the worker has crashed. However, in cases where the task is taking an abnormal amount of time, the worker will update the deadline accordingly.

Due to this system, it is adviced to set urgent and short-lived tasks to a shorter `maxTaskDuration`. But take network connectivity into consideration, as setting it very low (like 5 seconds) may cause the deadline to be reached before it can be prolonged.

If the task is dequeued, your task model gets a notification in `func onDequeueTask(withId taskId: ObjectId, withContext context: ExecutionContext, inQueue queue: MongoQueue) async throws`.

Likewise, on execution failure you get a call on `func onExecutionFailure(failureContext: QueuedTaskFailure<ExecutionContext>) async throws -> TaskExecutionFailureAction` where you can decide whether to requeue, and whether to apply a maximum amount of attempts.
