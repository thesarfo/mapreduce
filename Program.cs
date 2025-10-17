using mapreduce;
using MapReduce;

Console.WriteLine("MapReduce Framework\n");

Console.WriteLine("Select execution mode:");
Console.WriteLine("1. Sequential");
Console.WriteLine("2. Parallel (Multi-core)");
Console.WriteLine("3. Distributed Master");
Console.WriteLine("4. Distributed Worker");
Console.Write("\nEnter choice (1-4): ");

var choice = Console.ReadLine();

if (choice == "1")
{
    Console.WriteLine("\n=== SEQUENTIAL EXECUTION ===\n");
    var mapper = new Mapper();
    var reducer = new Reducer();
    var job = new MapReduceJob(mapper, reducer);
    job.ReadInput("input.txt", "output.txt");
    Console.WriteLine("\nJob completed successfully!");
}
else if (choice == "2")
{
    Console.WriteLine("\n=== PARALLEL EXECUTION ===\n");
    
    var mapper = new Mapper();
    var reducer = new Reducer();
    int numReducers = Environment.ProcessorCount; 
    int chunkSizeInMB = 1;
    
    var parallelJob = new ParallelMapReduceJob(mapper, reducer, numReducers, chunkSizeInMB);
    await parallelJob.ExecuteAsync("input.txt", "output.txt");
    Console.WriteLine("\nJob completed successfully!");
}
else if (choice == "3")
{
    Console.WriteLine("\n=== DISTRIBUTED MASTER ===\n");
    await MasterProgram.RunMasterAsync(args);
}
else if (choice == "4")
{
    Console.Write("Enter worker ID: ");
    var workerId = Console.ReadLine() ?? "worker-1";
    Console.WriteLine($"\n=== DISTRIBUTED WORKER ({workerId}) ===\n");
    await WorkerProgram.RunWorkerAsync(workerId);
}