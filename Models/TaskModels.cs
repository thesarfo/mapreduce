using System;
using System.Collections.Generic;

namespace MapReduce.Distributed.Models
{
    public enum TaskType
    {
        Map,
        Reduce
    }

    public enum MapReduceTaskStatus
    {
        Pending,
        Assigned,
        Running,
        Completed,
        Failed
    }

    public class MapReduceTask
    {
        public string TaskId { get; set; } = string.Empty;
        public TaskType Type { get; set; }
        public MapReduceTaskStatus Status { get; set; }
        public string? AssignedWorker { get; set; }
        public DateTime? AssignedAt { get; set; }
        public DateTime? CompletedAt { get; set; }
        public string? ErrorMessage { get; set; }
        
        public MapTaskData? MapData { get; set; }
        public ReduceTaskData? ReduceData { get; set; }
    }

    public class MapTaskData
    {
        public int ChunkId { get; set; }
        public List<string> Lines { get; set; } = new List<string>();
        public int StartLineNumber { get; set; }
        public int EndLineNumber { get; set; }
        public int NumReducers { get; set; }
    }

    public class ReduceTaskData
    {
        public int PartitionId { get; set; }
        public List<string> IntermediateFileUrls { get; set; } = new List<string>();
    }

    public class TaskRequest
    {
        public string WorkerId { get; set; } = string.Empty;
        public List<TaskType> SupportedTypes { get; set; } = new List<TaskType>();
    }

    public class TaskSubmission
    {
        public string TaskId { get; set; } = string.Empty;
        public string WorkerId { get; set; } = string.Empty;
        public bool Success { get; set; }
        public string? ErrorMessage { get; set; }
        public int RecordsProcessed { get; set; }
        public List<string>? OutputFileUrls { get; set; }
    }

    public class WorkerHeartbeat
    {
        public string WorkerId { get; set; } = string.Empty;
        public string Status { get; set; } = "idle";
        public int CurrentTaskCount { get; set; }
        public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    }
}

