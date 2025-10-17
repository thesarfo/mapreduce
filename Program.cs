using mapreduce;
using MapReduce;

Console.WriteLine("MapReduce Framework - Parallel Execution\n");

var mapper = new Mapper();
var reducer = new Reducer();

Console.WriteLine("Select execution mode:");
Console.WriteLine("1. Sequential");
Console.WriteLine("2. Parallel");
Console.Write("\nEnter choice (1 or 2): ");

var choice = Console.ReadLine();

if (choice == "1")
{
    Console.WriteLine("\n=== SEQUENTIAL EXECUTION ===\n");
    var job = new MapReduceJob(mapper, reducer);
    job.ReadInput("input.txt", "output.txt");
}
else
{
    Console.WriteLine("\n=== PARALLEL EXECUTION ===\n");
    
    int numReducers = Environment.ProcessorCount; 
    int chunkSizeInMB = 1;
    
    var parallelJob = new ParallelMapReduceJob(mapper, reducer, numReducers, chunkSizeInMB);
    await parallelJob.ExecuteAsync("input.txt", "output.txt");
}

Console.WriteLine("\nJob completed successfully!");