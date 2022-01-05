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
