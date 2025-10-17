using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using mapreduce;

namespace MapReduce
{
    public class ReduceTask
    {
        private readonly IReducer _reducer;
        private readonly int _partitionId;
        private readonly string _outputDirectory;

        public ReduceTask(IReducer reducer, int partitionId, string outputDirectory)
        {
            _reducer = reducer;
            _partitionId = partitionId;
            _outputDirectory = outputDirectory;
        }
        public async Task<ReduceTaskResult> ExecuteAsync(List<string> partitionFiles)
        {
            var result = new ReduceTaskResult
            {
                PartitionId = _partitionId,
                StartTime = DateTime.Now
            };

            try
            {
                // Read all intermediate key-value pairs from map outputs
                var intermediateData = new List<KeyValuePair<string, string>>();

                foreach (var partitionFile in partitionFiles)
                {
                    if (File.Exists(partitionFile))
                    {
                        foreach (var line in File.ReadLines(partitionFile))
                        {
                            var parts = line.Split('\t');
                            if (parts.Length == 2)
                            {
                                intermediateData.Add(new KeyValuePair<string, string>(parts[0], parts[1]));
                                result.RecordsRead++;
                            }
                        }
                    }
                }

                // Shuffle/Sort
                var grouped = intermediateData
                    .GroupBy(kv => kv.Key)
                    .OrderBy(g => g.Key) // Sort for consistent output
                    .ToDictionary(g => g.Key, g => g.Select(kv => kv.Value));

                // Output file for this reduce task
                string outputFile = Path.Combine(_outputDirectory, $"part-{_partitionId:D5}.txt");

                // Apply reduce function and write output
                using (var writer = new StreamWriter(outputFile))
                {
                    foreach (var kv in grouped)
                    {
                        string reducedValue = _reducer.Reduce(kv.Key, kv.Value);
                        await writer.WriteLineAsync($"{kv.Key}\t{reducedValue}");
                        result.RecordsEmitted++;
                    }
                }

                result.OutputFile = outputFile;
                result.EndTime = DateTime.Now;
                result.Success = true;

                return result;
            }
            catch (Exception ex)
            {
                result.EndTime = DateTime.Now;
                result.Success = false;
                result.ErrorMessage = ex.Message;
                return result;
            }
        }
    }

    public class ReduceTaskResult
    {
        public int PartitionId { get; set; }
        public bool Success { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int RecordsRead { get; set; }
        public int RecordsEmitted { get; set; }
        public string? OutputFile { get; set; }
        public string? ErrorMessage { get; set; }

        public TimeSpan Duration => EndTime - StartTime;

        public override string ToString()
        {
            return Success
                ? $"Reduce Task {PartitionId}: {RecordsRead} -> {RecordsEmitted} records in {Duration.TotalMilliseconds:F2}ms"
                : $"Reduce Task {PartitionId}: FAILED - {ErrorMessage}";
        }
    }
}

