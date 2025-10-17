using mapreduce;
using MapReduce.Distributed;

namespace MapReduce
{
    public class WorkerProgram
    {
        public static async Task RunWorkerAsync(string workerId, string masterUrl = "http://localhost:5000")
        {
            var mapper = new Mapper();
            var reducer = new Reducer();

            var worker = new WorkerNode(workerId, masterUrl, mapper, reducer);

            Console.WriteLine($"[Worker {workerId}] Connecting to master at {masterUrl}");
            Console.WriteLine($"[Worker {workerId}] Press Ctrl+C to stop");

            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (sender, e) =>
            {
                e.Cancel = true;
                worker.Stop();
                cts.Cancel();
            };

            try
            {
                await worker.StartAsync(cts.Token);
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine($"[Worker {workerId}] Shutdown complete");
            }
        }
    }
}

