using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using MapReduce.Distributed;
using MapReduce.Distributed.Models;
using System.Diagnostics;

namespace MapReduce
{
    public class MasterProgram
    {
        public static async Task RunMasterAsync(string[] args)
        {
            var totalStopwatch = Stopwatch.StartNew();

            var builder = WebApplication.CreateBuilder(args);

            builder.Services.AddControllers();
            builder.Services.AddEndpointsApiExplorer();
            builder.WebHost.UseUrls("http://localhost:5000");

            var app = builder.Build();
            app.MapControllers();

            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║         DISTRIBUTED MAPREDUCE EXECUTION                        ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝\n");

            var jobConfig = new JobConfig
            {
                InputPath = "input.txt",
                OutputPath = "output.txt",
                NumMappers = 10,
                NumReducers = 4,
                ChunkSizeInMB = 1
            };

            var distributedJob = new DistributedMapReduceJob(jobConfig);
            distributedJob.PrepareJob();

            MasterController.Initialize(distributedJob.GetMaster());

            Console.WriteLine("\n[Master] Server starting on http://localhost:5000");
            Console.WriteLine("[Master] Waiting for workers to connect...");
            Console.WriteLine("[Master] Press Ctrl+C to stop\n");

            var serverTask = app.RunAsync();

            var cts = new CancellationTokenSource();
            
            var monitorTask = Task.Run(async () =>
            {
                var success = await distributedJob.WaitForCompletionAsync(TimeSpan.FromMinutes(10));
                
                if (success)
                {
                    var finalRecords = await distributedJob.MergeOutputsAsync();
                    
                    totalStopwatch.Stop();

                    var finalStatus = distributedJob.GetMaster().GetJobStatus();
                    
                    Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
                    Console.WriteLine("║                      EXECUTION SUMMARY                         ║");
                    Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
                    Console.WriteLine($"Total execution time:     {totalStopwatch.ElapsedMilliseconds}ms ({totalStopwatch.Elapsed.TotalSeconds:F1}s)");
                    Console.WriteLine($"Total tasks:              {finalStatus.TotalTasks}");
                    Console.WriteLine($"  Map tasks:              {finalStatus.TotalTasks - jobConfig.NumReducers}");
                    Console.WriteLine($"  Reduce tasks:           {jobConfig.NumReducers}");
                    Console.WriteLine($"Completed tasks:          {finalStatus.CompletedTasks}");
                    Console.WriteLine($"Failed tasks:             {finalStatus.FailedTasks}");
                    Console.WriteLine($"Active workers:           {finalStatus.ActiveWorkers}");
                    Console.WriteLine($"Final records:            {finalRecords:N0}");
                    Console.WriteLine($"Output file:              {jobConfig.OutputPath}");
                    Console.WriteLine($"Execution mode:           Distributed (master-worker)");
                    Console.WriteLine();
                    Console.WriteLine("Job completed successfully!");
                    Console.WriteLine("\n[Master] Server still running. Press Ctrl+C to shutdown.");
                }
                else
                {
                    Console.WriteLine("\n[Master] Job failed or timed out");
                }
            });

            await serverTask;
        }
    }
}

