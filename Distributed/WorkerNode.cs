using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using mapreduce;
using MapReduce.Distributed.Models;

namespace MapReduce.Distributed
{
    public class WorkerNode
    {
        private readonly string _workerId;
        private readonly string _masterUrl;
        private readonly IMapper _mapper;
        private readonly IReducer _reducer;
        private readonly HttpClient _httpClient;
        private bool _running;

        public WorkerNode(string workerId, string masterUrl, IMapper mapper, IReducer reducer)
        {
            _workerId = workerId;
            _masterUrl = masterUrl;
            _mapper = mapper;
            _reducer = reducer;
            _httpClient = new HttpClient();
        }

        public async Task StartAsync(CancellationToken cancellationToken = default)
        {
            _running = true;
            Console.WriteLine($"[Worker {_workerId}] Started");

            var heartbeatTask = SendHeartbeatsAsync(cancellationToken);
            var workTask = ProcessTasksAsync(cancellationToken);

            await Task.WhenAll(heartbeatTask, workTask);
        }

        public void Stop()
        {
            _running = false;
            Console.WriteLine($"[Worker {_workerId}] Stopping");
        }

        private async Task SendHeartbeatsAsync(CancellationToken cancellationToken)
        {
            while (_running && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var heartbeat = new WorkerHeartbeat
                    {
                        WorkerId = _workerId,
                        Status = "idle",
                        Timestamp = DateTime.UtcNow
                    };

                    await _httpClient.PostAsJsonAsync($"{_masterUrl}/api/master/heartbeat", heartbeat, cancellationToken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {_workerId}] Heartbeat failed: {ex.Message}");
                }

                await Task.Delay(5000, cancellationToken);
            }
        }

        private async Task ProcessTasksAsync(CancellationToken cancellationToken)
        {
            while (_running && !cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var request = new TaskRequest
                    {
                        WorkerId = _workerId,
                        SupportedTypes = new List<TaskType> { TaskType.Map, TaskType.Reduce }
                    };

                    var response = await _httpClient.PostAsJsonAsync($"{_masterUrl}/api/master/request-task", request, cancellationToken);

                    if (response.StatusCode == System.Net.HttpStatusCode.NoContent)
                    {
                        await Task.Delay(2000, cancellationToken);
                        continue;
                    }

                    var task = await response.Content.ReadFromJsonAsync<MapReduceTask>(cancellationToken: cancellationToken);
                    
                    if (task != null)
                    {
                        Console.WriteLine($"[Worker {_workerId}] Received {task.Type} task {task.TaskId}");
                        await ExecuteTaskAsync(task, cancellationToken);
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {_workerId}] Error: {ex.Message}");
                    await Task.Delay(5000, cancellationToken);
                }
            }
        }

        private async Task ExecuteTaskAsync(MapReduceTask task, CancellationToken cancellationToken)
        {
            var submission = new TaskSubmission
            {
                TaskId = task.TaskId,
                WorkerId = _workerId,
                Success = false
            };

            try
            {
                if (task.Type == TaskType.Map && task.MapData != null)
                {
                    submission.RecordsProcessed = await ExecuteMapTaskAsync(task.MapData);
                    submission.Success = true;
                }
                else if (task.Type == TaskType.Reduce && task.ReduceData != null)
                {
                    submission.RecordsProcessed = await ExecuteReduceTaskAsync(task.ReduceData);
                    submission.Success = true;
                }
            }
            catch (Exception ex)
            {
                submission.Success = false;
                submission.ErrorMessage = ex.Message;
                Console.WriteLine($"[Worker {_workerId}] Task {task.TaskId} failed: {ex.Message}");
            }

            await _httpClient.PostAsJsonAsync($"{_masterUrl}/api/master/submit-task", submission, cancellationToken);
        }

        private async Task<int> ExecuteMapTaskAsync(MapTaskData mapData)
        {
            int recordsEmitted = 0;
            var partitionWriters = new Dictionary<int, StreamWriter>();
            var baseDir = Path.Combine(Path.GetTempPath(), "mapreduce");
            var outputDir = Path.Combine(baseDir, $"map_task_{mapData.ChunkId}");
            Directory.CreateDirectory(outputDir);
            
            Console.WriteLine($"[Worker {_workerId}] Writing map output to: {outputDir}");

            try
            {
                for (int i = 0; i < mapData.NumReducers; i++)
                {
                    var partitionFile = Path.Combine(outputDir, $"partition_{i}.txt");
                    partitionWriters[i] = new StreamWriter(partitionFile);
                }

                int lineNumber = mapData.StartLineNumber;
                foreach (var line in mapData.Lines)
                {
                    var mapResult = _mapper.Map(lineNumber.ToString(), line);

                    foreach (var kv in mapResult)
                    {
                        int partition = Math.Abs(kv.Key.GetHashCode()) % mapData.NumReducers;
                        await partitionWriters[partition].WriteLineAsync($"{kv.Key}\t{kv.Value}");
                        recordsEmitted++;
                    }

                    lineNumber++;
                }
            }
            finally
            {
                foreach (var writer in partitionWriters.Values)
                {
                    await writer.FlushAsync();
                    writer.Close();
                }
            }

            Console.WriteLine($"[Worker {_workerId}] Map task complete: {recordsEmitted} records emitted");
            return recordsEmitted;
        }

        private async Task<int> ExecuteReduceTaskAsync(ReduceTaskData reduceData)
        {
            Console.WriteLine($"[Worker {_workerId}] Starting reduce task for partition {reduceData.PartitionId}");
            Console.WriteLine($"[Worker {_workerId}] Need to fetch {reduceData.IntermediateFileUrls.Count} intermediate files");
            
            var intermediateData = new List<KeyValuePair<string, string>>();

            foreach (var fileUrl in reduceData.IntermediateFileUrls)
            {
                try
                {
                    Console.WriteLine($"[Worker {_workerId}] Fetching: {fileUrl}");
                    var content = await _httpClient.GetStringAsync(fileUrl);
                    var lines = content.Split('\n', StringSplitOptions.RemoveEmptyEntries);
                    
                    foreach (var line in lines)
                    {
                        var parts = line.Split('\t');
                        if (parts.Length == 2)
                        {
                            intermediateData.Add(new KeyValuePair<string, string>(parts[0], parts[1]));
                        }
                    }
                    Console.WriteLine($"[Worker {_workerId}] Fetched {lines.Length} records from {fileUrl}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Worker {_workerId}] ERROR fetching {fileUrl}: {ex.Message}");
                }
            }

            Console.WriteLine($"[Worker {_workerId}] Grouping {intermediateData.Count} intermediate records");
            
            var grouped = intermediateData
                .GroupBy(kv => kv.Key)
                .OrderBy(g => g.Key)
                .ToDictionary(g => g.Key, g => g.Select(kv => kv.Value));

            Console.WriteLine($"[Worker {_workerId}] Grouped into {grouped.Count} unique keys");

            var baseDir = Path.Combine(Path.GetTempPath(), "mapreduce");
            var outputDir = Path.Combine(baseDir, $"reduce_task_{reduceData.PartitionId}");
            Directory.CreateDirectory(outputDir);
            var outputFile = Path.Combine(outputDir, "output.txt");

            Console.WriteLine($"[Worker {_workerId}] Writing reduce output to: {outputFile}");

            int recordsEmitted = 0;
            using (var writer = new StreamWriter(outputFile))
            {
                foreach (var kv in grouped)
                {
                    string reducedValue = _reducer.Reduce(kv.Key, kv.Value);
                    await writer.WriteLineAsync($"{kv.Key}\t{reducedValue}");
                    recordsEmitted++;
                }
            }

            Console.WriteLine($"[Worker {_workerId}] Reduce task complete: {recordsEmitted} records emitted");
            return recordsEmitted;
        }
    }
}

