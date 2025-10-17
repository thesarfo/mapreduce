using System.Text.Json;

namespace MapReduce.Distributed.Models
{
    public class JobConfig
    {
        public string JobId { get; set; } = Guid.NewGuid().ToString();
        public string InputPath { get; set; } = string.Empty;
        public string OutputPath { get; set; } = string.Empty;
        public int NumMappers { get; set; } = 10;
        public int NumReducers { get; set; } = 4;
        public int ChunkSizeInMB { get; set; } = 16;
        public string WorkingDirectory { get; set; } = "/tmp/mapreduce";

        public static JobConfig FromJson(string json)
        {
            return JsonSerializer.Deserialize<JobConfig>(json) ?? new JobConfig();
        }

        public string ToJson()
        {
            return JsonSerializer.Serialize(this, new JsonSerializerOptions { WriteIndented = true });
        }

        public string GetJobDirectory()
        {
            return Path.Combine(WorkingDirectory, JobId);
        }

        public string GetMapTaskDirectory(int taskId)
        {
            return Path.Combine(GetJobDirectory(), $"map_task_{taskId}");
        }

        public string GetReduceTaskDirectory(int taskId)
        {
            return Path.Combine(GetJobDirectory(), $"reduce_task_{taskId}");
        }
    }
}

