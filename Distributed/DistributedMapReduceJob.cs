using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using MapReduce.Distributed.Models;

namespace MapReduce.Distributed
{
    public class DistributedMapReduceJob
    {
        private readonly JobConfig _config;
        private readonly MasterNode _master;

        public DistributedMapReduceJob(JobConfig config)
        {
            _config = config;
            _master = new MasterNode(config);
        }

        public MasterNode GetMaster() => _master;

        public void PrepareJob()
        {
            Console.WriteLine($"[Job {_config.JobId}] Preparing distributed job");
            Console.WriteLine($"  Input: {_config.InputPath}");
            Console.WriteLine($"  Output: {_config.OutputPath}");
            Console.WriteLine($"  Mappers: {_config.NumMappers}");
            Console.WriteLine($"  Reducers: {_config.NumReducers}");

            Directory.CreateDirectory(_config.GetJobDirectory());

            var mapTasks = CreateMapTasks();
            _master.RegisterTasks(mapTasks);

            Console.WriteLine($"[Job {_config.JobId}] Registered {mapTasks.Count} map tasks");
            Console.WriteLine($"[Job {_config.JobId}] Reduce tasks will be created after all map tasks complete");
        }

        public void CreateReduceTasksAfterMaps()
        {
            var reduceTasks = CreateReduceTasks();
            _master.RegisterTasks(reduceTasks);
            Console.WriteLine($"[Job {_config.JobId}] Registered {reduceTasks.Count} reduce tasks");
        }

        private List<MapReduceTask> CreateMapTasks()
        {
            var splitter = new InputSplitter(_config.ChunkSizeInMB);
            var chunks = splitter.SplitFile(_config.InputPath);

            var mapTasks = new List<MapReduceTask>();

            for (int i = 0; i < chunks.Count; i++)
            {
                var chunk = chunks[i];
                var task = new MapReduceTask
                {
                    TaskId = $"map_{i}",
                    Type = TaskType.Map,
                    Status = MapReduceTaskStatus.Pending,
                    MapData = new MapTaskData
                    {
                        ChunkId = i,
                        Lines = chunk.Lines,
                        StartLineNumber = chunk.StartLineNumber,
                        EndLineNumber = chunk.EndLineNumber,
                        NumReducers = _config.NumReducers
                    }
                };

                mapTasks.Add(task);
            }

            return mapTasks;
        }

        private List<MapReduceTask> CreateReduceTasks()
        {
            var reduceTasks = new List<MapReduceTask>();

            for (int i = 0; i < _config.NumReducers; i++)
            {
                var intermediateUrls = _master.GetIntermediateFileUrls(i);

                var task = new MapReduceTask
                {
                    TaskId = $"reduce_{i}",
                    Type = TaskType.Reduce,
                    Status = MapReduceTaskStatus.Pending,
                    ReduceData = new ReduceTaskData
                    {
                        PartitionId = i,
                        IntermediateFileUrls = intermediateUrls
                    }
                };

                reduceTasks.Add(task);
            }

            return reduceTasks;
        }

        public async Task<bool> WaitForCompletionAsync(TimeSpan timeout)
        {
            var startTime = DateTime.UtcNow;
            var checkInterval = TimeSpan.FromSeconds(3);
            bool reducesCreated = false;
            var lastCompletedCount = 0;

            Console.WriteLine("MONITORING JOB PROGRESS");
            Console.WriteLine("─────────────────────────────────────────────────────────────────");

            while (DateTime.UtcNow - startTime < timeout)
            {
                var status = _master.GetJobStatus();
                var elapsed = DateTime.UtcNow - startTime;
                
                if (status.CompletedTasks != lastCompletedCount)
                {
                    var progress = status.TotalTasks > 0 ? (status.CompletedTasks * 100.0 / status.TotalTasks) : 0;
                    Console.WriteLine($"[{elapsed.TotalSeconds:F0}s] Progress: {status.CompletedTasks}/{status.TotalTasks} ({progress:F1}%) | Running: {status.RunningTasks} | Failed: {status.FailedTasks} | Workers: {status.ActiveWorkers}");
                    lastCompletedCount = status.CompletedTasks;
                }

                if (!reducesCreated && _master.AreAllMapTasksComplete())
                {
                    Console.WriteLine($"\n[{elapsed.TotalSeconds:F0}s] All map tasks complete! Creating reduce tasks...\n");
                    CreateReduceTasksAfterMaps();
                    reducesCreated = true;
                }

                if (status.IsComplete)
                {
                    Console.WriteLine($"\n[{elapsed.TotalSeconds:F0}s] All tasks completed!\n");
                    return status.FailedTasks == 0;
                }

                await Task.Delay(checkInterval);
            }

            Console.WriteLine($"\n[Master] Job timed out after {timeout.TotalSeconds}s");
            return false;
        }

        public async Task<int> MergeOutputsAsync()
        {
            Console.WriteLine("MERGING OUTPUTS");
            Console.WriteLine("─────────────────────────────────────────────────────────────────");
            
            int totalRecords = 0;

            using (var writer = new StreamWriter(_config.OutputPath))
            {
                for (int i = 0; i < _config.NumReducers; i++)
                {
                    var baseDir = Path.Combine(Path.GetTempPath(), "mapreduce");
                    var reduceOutputDir = Path.Combine(baseDir, $"reduce_task_{i}");
                    var outputFile = Path.Combine(reduceOutputDir, "output.txt");

                    if (File.Exists(outputFile))
                    {
                        int partitionRecords = 0;
                        foreach (var line in File.ReadLines(outputFile))
                        {
                            await writer.WriteLineAsync(line);
                            partitionRecords++;
                            totalRecords++;
                        }
                        Console.WriteLine($"  Merged partition {i}: {partitionRecords:N0} records");
                    }
                }
            }

            Console.WriteLine($"Output merged to {_config.OutputPath}: {totalRecords:N0} total records\n");
            return totalRecords;
        }
    }
}

