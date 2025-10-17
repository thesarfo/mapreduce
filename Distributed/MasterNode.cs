using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using MapReduce.Distributed.Models;

namespace MapReduce.Distributed
{
    public class MasterNode
    {
        private readonly ConcurrentDictionary<string, MapReduceTask> _tasks = new();
        private readonly ConcurrentDictionary<string, WorkerInfo> _workers = new();
        private readonly JobConfig _jobConfig;
        private readonly object _assignmentLock = new();

        public MasterNode(JobConfig jobConfig)
        {
            _jobConfig = jobConfig;
        }

        public void RegisterTasks(List<MapReduceTask> tasks)
        {
            foreach (var task in tasks)
            {
                _tasks[task.TaskId] = task;
            }
        }

        public MapReduceTask? RequestTask(TaskRequest request)
        {
            lock (_assignmentLock)
            {
                UpdateWorkerInfo(request.WorkerId);

                var availableTask = _tasks.Values
                    .Where(t => t.Status == MapReduceTaskStatus.Pending)
                    .Where(t => request.SupportedTypes.Contains(t.Type))
                    .OrderBy(t => t.Type == TaskType.Map ? 0 : 1)
                    .FirstOrDefault();

                if (availableTask != null)
                {
                    availableTask.Status = MapReduceTaskStatus.Assigned;
                    availableTask.AssignedWorker = request.WorkerId;
                    availableTask.AssignedAt = DateTime.UtcNow;
                    
                    Console.WriteLine($"[Master] Assigned {availableTask.Type} task {availableTask.TaskId} to worker {request.WorkerId}");
                }

                return availableTask;
            }
        }

        public void SubmitTaskResult(TaskSubmission submission)
        {
            if (_tasks.TryGetValue(submission.TaskId, out var task))
            {
                task.Status = submission.Success ? MapReduceTaskStatus.Completed : MapReduceTaskStatus.Failed;
                task.CompletedAt = DateTime.UtcNow;
                task.ErrorMessage = submission.ErrorMessage;

                var duration = task.CompletedAt - task.AssignedAt;
                Console.WriteLine($"[Master] Task {submission.TaskId} completed by {submission.WorkerId} in {duration?.TotalSeconds:F2}s - Status: {task.Status}");

                if (!submission.Success)
                {
                    Console.WriteLine($"[Master] Task {submission.TaskId} failed: {submission.ErrorMessage}");
                }
            }
        }

        public void ProcessHeartbeat(WorkerHeartbeat heartbeat)
        {
            UpdateWorkerInfo(heartbeat.WorkerId, heartbeat);
        }

        private void UpdateWorkerInfo(string workerId, WorkerHeartbeat? heartbeat = null)
        {
            _workers.AddOrUpdate(
                workerId,
                new WorkerInfo { WorkerId = workerId, LastHeartbeat = DateTime.UtcNow },
                (key, existing) =>
                {
                    existing.LastHeartbeat = DateTime.UtcNow;
                    if (heartbeat != null)
                    {
                        existing.Status = heartbeat.Status;
                        existing.CurrentTaskCount = heartbeat.CurrentTaskCount;
                    }
                    return existing;
                });
        }

        public JobStatus GetJobStatus()
        {
            var totalTasks = _tasks.Count;
            var completedTasks = _tasks.Values.Count(t => t.Status == MapReduceTaskStatus.Completed);
            var failedTasks = _tasks.Values.Count(t => t.Status == MapReduceTaskStatus.Failed);
            var runningTasks = _tasks.Values.Count(t => t.Status == MapReduceTaskStatus.Running || t.Status == MapReduceTaskStatus.Assigned);

            return new JobStatus
            {
                JobId = _jobConfig.JobId,
                TotalTasks = totalTasks,
                CompletedTasks = completedTasks,
                FailedTasks = failedTasks,
                RunningTasks = runningTasks,
                ActiveWorkers = _workers.Count,
                IsComplete = completedTasks + failedTasks == totalTasks
            };
        }

        public bool AreAllMapTasksComplete()
        {
            var mapTasks = _tasks.Values.Where(t => t.Type == TaskType.Map).ToList();
            if (mapTasks.Count == 0) return false;
            
            return mapTasks.All(t => t.Status == MapReduceTaskStatus.Completed);
        }

        public List<string> GetIntermediateFileUrls(int partitionId)
        {
            return _tasks.Values
                .Where(t => t.Type == TaskType.Map && t.Status == MapReduceTaskStatus.Completed)
                .Select(t => $"http://localhost:5000/files/{_jobConfig.JobId}/map_task_{t.MapData?.ChunkId}/partition_{partitionId}.txt")
                .ToList();
        }
    }

    public class WorkerInfo
    {
        public string WorkerId { get; set; } = string.Empty;
        public DateTime LastHeartbeat { get; set; }
        public string Status { get; set; } = "idle";
        public int CurrentTaskCount { get; set; }
    }

    public class JobStatus
    {
        public string JobId { get; set; } = string.Empty;
        public int TotalTasks { get; set; }
        public int CompletedTasks { get; set; }
        public int FailedTasks { get; set; }
        public int RunningTasks { get; set; }
        public int ActiveWorkers { get; set; }
        public bool IsComplete { get; set; }
    }
}

