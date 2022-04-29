import Logging
import MongoCore
import Foundation
import Meow
import Foundation

public struct TaskPriority {
    internal enum _Raw: Int, Codable {
        case relaxed = -2, lower = -1, normal = 0, higher = 1, urgent = 2
    }
    
    internal let raw: _Raw
    
    /// Take your time, it's expected to take a while
    public static let relaxed = TaskPriority(raw: .relaxed)
    
    /// Not as urgent as regular user actions, but please do not take all the time in the world
    public static let lower = TaskPriority(raw: .lower)
    
    /// Regular user actions
    public static let normal = TaskPriority(raw: .normal)
    
    /// This is needed fast, think of real-time communication
    public static let higher = TaskPriority(raw: .higher)
    
    /// THIS SHOULDN'T WAIT
    /// Though, if something is to be executed _immediately_, you probably shouldn't use a job queue
    public static let urgent = TaskPriority(raw: .urgent)
}


/// The current status of your task
public struct TaskStatus {
    internal enum _Raw: String, Codable {
        case scheduled
        case suspended
        case executing
        case dequeued
    }
    
    internal let raw: _Raw
    
    /// The task is scheduled, and is ready for execution
    public static let scheduled = TaskStatus(raw: .scheduled)
    
    /// The task has been suspended until further action
    public static let suspended = TaskStatus(raw: .suspended)
    
    /// The task is currently executing
    public static let executing = TaskStatus(raw: .executing)
    
    /// The task is dequeued / soft deleted
    public static let dequeued = TaskStatus(raw: .dequeued)
}

public struct TaskModel: Codable {
    let _id: ObjectId
    
    /// Contains `Task.name`
    let category: String
    let group: String?
    let uniqueKey: String?
    
    let creationDate: Date
    let priority: TaskPriority._Raw
    var executeAfter: Date
    var executeBefore: Date?
    var attempts: Int
    var status: TaskStatus._Raw
    var metadata: Document
    
    struct ExecutingContext: Codable {
        /// Used to represent when the task was first started. Normally it's equal to `executionStartDate`
        /// But when a task takes an unexpectedly long amount of time, the two values will be different
        let startDate: Date
        
        /// If `status == .executing`, this marks the start timestamp
        /// This allows tasks to be rebooted if the executioner process crashed
        /// If the current date exceeds `executionStartDate + maxTaskDuration`, the task likely crashed
        /// The executioner of the task **MUST** refresh this date at least every `maxTaskDuration` interval to ensure other executioners don't pick up on the task
        var lastUpdate: Date
        
        init() {
            let now = Date()
            self.startDate = now
            self.lastUpdate = now
        }
        
        mutating func updateActivity() {
            lastUpdate = Date()
        }
    }
    
    var execution: ExecutingContext?
    let maxTaskDuration: TimeInterval
//    let allowsParallelisation: Bool
    
    private enum ConfigurationType: String, Codable {
        case scheduled, recurring
    }
    
    private let configurationType: ConfigurationType
    private let configuration: Document
    
    init<T: _QueuedTask>(representing task: T) throws {
        assert(task.maxTaskDuration >= 30, "maxTaskDuration is set unreasonably low in category \(T.category): \(task.maxTaskDuration)")
        
        self._id = ObjectId()
        self.category = T.category
        self.group = task.group
        self.priority = task.priority.raw
        self.attempts = 0
        self.creationDate = Date()
        self.status = .scheduled
        self.metadata = try BSONEncoder().encode(task)
        self.maxTaskDuration = task.maxTaskDuration
        
        switch task.configuration.value {
        case .scheduled(let configuration):
            self.configurationType = .scheduled
            self.uniqueKey = nil
            self.executeAfter = configuration.scheduledDate
            self.executeBefore = configuration.executeBefore
            self.configuration = try BSONEncoder().encode(configuration)
        case .recurring(let configuration):
            self.configurationType = .recurring
            self.uniqueKey = configuration.key
            self.executeAfter = configuration.scheduledDate
            self.executeBefore = configuration.deadline.map { deadline in
                configuration.scheduledDate.addingTimeInterval(deadline)
            }
            self.configuration = try BSONEncoder().encode(configuration)
        }
    }
    
    func readConfiguration() throws -> _TaskConfiguration {
        switch configurationType {
        case .scheduled:
            return try _TaskConfiguration(
                value: .scheduled(
                    BSONDecoder().decode(ScheduledTaskConfiguration.self, from: configuration)
                )
            )
        case .recurring:
            return try _TaskConfiguration(
                value: .recurring(
                    BSONDecoder().decode(RecurringTaskConfiguration.self, from: configuration)
                )
            )
        }
    }
}

public struct _TaskConfiguration {
    internal enum _TaskConfiguration {
        case scheduled(ScheduledTaskConfiguration)
        case recurring(RecurringTaskConfiguration)
    }
    
    internal var value: _TaskConfiguration
    
    internal init(value: _TaskConfiguration) {
        self.value = value
    }
}

struct RecurringTaskConfiguration: Codable {
    let scheduledDate: Date
    let key: String
    let deadline: TimeInterval?
}

public struct ScheduledTaskConfiguration: Codable {
    let scheduledDate: Date
    let executeBefore: Date?
}
