using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using mapreduce;

namespace MapReduce
{
    public class ParallelMapReduceJob
    {
        private readonly IMapper _mapper;
        private readonly IReducer _reducer;
        private readonly int _numReducers;
        private readonly int _chunkSizeInMB;
        private readonly string _tempDirectory;

        public ParallelMapReduceJob(
            IMapper mapper, 
            IReducer reducer, 
            int numReducers = 4,
            int chunkSizeInMB = 16)
        {
            _mapper = mapper;
            _reducer = reducer;
            _numReducers = numReducers;
            _chunkSizeInMB = chunkSizeInMB;
            _tempDirectory = Path.Combine(Path.GetTempPath(), $"mapreduce_{Guid.NewGuid()}");
            
            Directory.CreateDirectory(_tempDirectory);
        }

        public async Task ExecuteAsync(string inputFile, string outputFile)
        {
            var totalStopwatch = Stopwatch.StartNew();

            try
            {
                Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
                Console.WriteLine("║         PARALLEL MAPREDUCE EXECUTION                           ║");
                Console.WriteLine("╚════════════════════════════════════════════════════════════════╝\n");

                Console.WriteLine("SPLITTING INPUT");
                Console.WriteLine("─────────────────────────────────────────────────────────────────");
                var splitter = new InputSplitter(_chunkSizeInMB);
                var chunks = splitter.SplitFile(inputFile);
                Console.WriteLine($"Split into {chunks.Count} chunks (chunk size: {_chunkSizeInMB} MB)");
                foreach (var chunk in chunks)
                {
                    Console.WriteLine($"  {chunk}");
                }
                Console.WriteLine();

                
                Console.WriteLine("PARALLEL MAP PHASE");
                Console.WriteLine("─────────────────────────────────────────────────────────────────");
                Console.WriteLine($"Running {chunks.Count} map tasks in parallel across CPU cores...\n");

                var mapStopwatch = Stopwatch.StartNew();
                var mapTasks = chunks.Select(chunk =>
                {
                    var mapTask = new MapTask(_mapper, _numReducers, _tempDirectory);
                    return Task.Run(async () =>
                    {
                        var result = await mapTask.ExecuteAsync(chunk);
                        Console.WriteLine($"{result}");
                        return result;
                    });
                }).ToList();

                var mapResults = await Task.WhenAll(mapTasks);
                mapStopwatch.Stop();

                int totalMapRecords = mapResults.Sum(r => r.RecordsEmitted);
                Console.WriteLine($"\nMap phase complete: {totalMapRecords:N0} intermediate records in {mapStopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"  Parallelization speedup: {chunks.Count} chunks processed concurrently\n");

                Console.WriteLine("SHUFFLE/PARTITION");
                Console.WriteLine("─────────────────────────────────────────────────────────────────");
                Console.WriteLine($"Data partitioned into {_numReducers} partitions using hash partitioning");
                Console.WriteLine($"  Partition function: partition = Math.Abs(key.GetHashCode()) % {_numReducers}");
                
                var reducerInputFiles = new Dictionary<int, List<string>>();
                for (int i = 0; i < _numReducers; i++)
                {
                    reducerInputFiles[i] = new List<string>();
                }

                foreach (var mapResult in mapResults)
                {
                    foreach (var kv in mapResult.PartitionFiles)
                    {
                        int partition = kv.Key;
                        string file = kv.Value;
                        reducerInputFiles[partition].Add(file);
                    }
                }

                Console.WriteLine($"Shuffle complete: {_numReducers} reducers ready\n");

                Console.WriteLine("PARALLEL REDUCE PHASE");
                Console.WriteLine("─────────────────────────────────────────────────────────────────");
                Console.WriteLine($"Running {_numReducers} reduce tasks in parallel...\n");

                string outputDir = Path.GetDirectoryName(outputFile) ?? ".";
                string tempOutputDir = Path.Combine(outputDir, $"temp_output_{Guid.NewGuid()}");
                Directory.CreateDirectory(tempOutputDir);

                var reduceStopwatch = Stopwatch.StartNew();
                var reduceTasks = Enumerable.Range(0, _numReducers).Select(partitionId =>
                {
                    var reduceTask = new ReduceTask(_reducer, partitionId, tempOutputDir);
                    return Task.Run(async () =>
                    {
                        var result = await reduceTask.ExecuteAsync(reducerInputFiles[partitionId]);
                        Console.WriteLine($"{result}");
                        return result;
                    });
                }).ToList();

                var reduceResults = await Task.WhenAll(reduceTasks);
                reduceStopwatch.Stop();

                int totalReduceRecords = reduceResults.Sum(r => r.RecordsEmitted);
                Console.WriteLine($"\nReduce phase complete: {totalReduceRecords:N0} final records in {reduceStopwatch.ElapsedMilliseconds}ms\n");

                Console.WriteLine("MERGING OUTPUT");
                Console.WriteLine("─────────────────────────────────────────────────────────────────");
                var outputFiles = reduceResults
                    .Where(r => r.OutputFile != null)
                    .Select(r => r.OutputFile!)
                    .ToList();
                await MergeOutputFiles(outputFiles, outputFile);
                Console.WriteLine($"Output written to: {outputFile}\n");

                totalStopwatch.Stop();

                
                Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
                Console.WriteLine("║                      EXECUTION SUMMARY                         ║");
                Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
                Console.WriteLine($"Total execution time:     {totalStopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"  Map phase:              {mapStopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"  Reduce phase:           {reduceStopwatch.ElapsedMilliseconds}ms");
                Console.WriteLine($"Map tasks:                {chunks.Count} (parallel)");
                Console.WriteLine($"Reduce tasks:             {_numReducers} (parallel)");
                Console.WriteLine($"Intermediate records:     {totalMapRecords:N0}");
                Console.WriteLine($"Final records:            {totalReduceRecords:N0}");
                Console.WriteLine($"CPU cores utilized:       {Environment.ProcessorCount}");
                Console.WriteLine();

                Directory.Delete(tempOutputDir, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"\nError: {ex.Message}");
                Console.WriteLine(ex.StackTrace);
                throw;
            }
            finally
            {
                if (Directory.Exists(_tempDirectory))
                {
                    Directory.Delete(_tempDirectory, true);
                }
            }
        }

        private async Task MergeOutputFiles(List<string> outputFiles, string finalOutputFile)
        {
            using (var writer = new StreamWriter(finalOutputFile))
            {
                foreach (var file in outputFiles.OrderBy(f => f))
                {
                    if (File.Exists(file))
                    {
                        foreach (var line in File.ReadLines(file))
                        {
                            await writer.WriteLineAsync(line);
                        }
                    }
                }
            }
        }
    }
}

