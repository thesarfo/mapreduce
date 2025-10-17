using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using mapreduce;

namespace MapReduce
{
    public class MapTask
    {
        private readonly IMapper _mapper;
        private readonly int _numReducers;
        private readonly string _tempDirectory;

        public MapTask(IMapper mapper, int numReducers, string tempDirectory)
        {
            _mapper = mapper;
            _numReducers = numReducers;
            _tempDirectory = tempDirectory;
        }

        public async Task<MapTaskResult> ExecuteAsync(InputChunk chunk)
        {
            var result = new MapTaskResult
            {
                ChunkId = chunk.ChunkId,
                StartTime = DateTime.Now
            };

            try
            {
                // Create partition writers (one file per reducer)
                var partitionWriters = new Dictionary<int, StreamWriter>();
                var partitionFiles = new Dictionary<int, string>();

                for (int i = 0; i < _numReducers; i++)
                {
                    string partitionFile = Path.Combine(_tempDirectory, $"map_{chunk.ChunkId}_partition_{i}.txt");
                    partitionFiles[i] = partitionFile;
                    partitionWriters[i] = new StreamWriter(partitionFile);
                }

                int lineNumber = chunk.StartLineNumber;

                // Process each line in the chunk
                foreach (var line in chunk.Lines)
                {
                    // Apply mapper
                    var mapOutput = _mapper.Map(lineNumber.ToString(), line);

                    // Partition and write each key-value pair
                    foreach (var kv in mapOutput)
                    {
                        int partition = GetPartition(kv.Key, _numReducers);
                        
                        // Write to the appropriate partition file
                        await partitionWriters[partition].WriteLineAsync($"{kv.Key}\t{kv.Value}");
                        
                        result.RecordsEmitted++;
                    }

                    lineNumber++;
                }

                // Close all writers
                foreach (var writer in partitionWriters.Values)
                {
                    await writer.FlushAsync();
                    writer.Close();
                }

                result.EndTime = DateTime.Now;
                result.Success = true;
                result.PartitionFiles = partitionFiles;

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

        private int GetPartition(string key, int numReducers)
        {
            return Math.Abs(key.GetHashCode()) % numReducers;
        }
    }

    public class MapTaskResult
    {
        public int ChunkId { get; set; }
        public bool Success { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int RecordsEmitted { get; set; }
        public Dictionary<int, string> PartitionFiles { get; set; } = new Dictionary<int, string>();
        public string? ErrorMessage { get; set; }

        public TimeSpan Duration => EndTime - StartTime;

        public override string ToString()
        {
            return Success 
                ? $"Map Task {ChunkId}: {RecordsEmitted} records in {Duration.TotalMilliseconds:F2}ms"
                : $"Map Task {ChunkId}: FAILED - {ErrorMessage}";
        }
    }
}

