using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using mapreduce;

namespace MapReduce
{
    public class MapReduceJob
    {
        private readonly IMapper _mapper;
        private readonly IReducer _reducer;

        public MapReduceJob(IMapper mapper, IReducer reducer)
        {
            _mapper = mapper;
            _reducer = reducer;
        }

        public void ReadInput(string filePath, string outputPath)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"File not found: {filePath}");

            var totalStopwatch = Stopwatch.StartNew();

            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║         SEQUENTIAL MAPREDUCE EXECUTION                         ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝\n");

            int lineNumber = 0;
            var intermediateResults = new List<KeyValuePair<string, string>>();

            // Map Phase
            Console.WriteLine("MAP PHASE");
            Console.WriteLine("─────────────────────────────────────────────────────────────────");
            var mapStopwatch = Stopwatch.StartNew();
            
            foreach (var line in File.ReadLines(filePath))
            {
                var mapResult = _mapper.Map(lineNumber.ToString(), line);

                foreach (var kv in mapResult)
                {
                    intermediateResults.Add(new KeyValuePair<string, string>(kv.Key, kv.Value));
                }

                lineNumber++;
            }
            
            mapStopwatch.Stop();
            Console.WriteLine($"Map phase complete: {intermediateResults.Count:N0} intermediate records in {mapStopwatch.ElapsedMilliseconds}ms\n");

            Console.WriteLine("SHUFFLE PHASE");
            Console.WriteLine("─────────────────────────────────────────────────────────────────");
            var shuffleStopwatch = Stopwatch.StartNew();
            
            var grouped = intermediateResults
                .GroupBy(kv => kv.Key)
                .ToDictionary(g => g.Key, g => g.Select(v => v.Value));
            
            shuffleStopwatch.Stop();
            Console.WriteLine($"Shuffle complete: {grouped.Count:N0} unique keys in {shuffleStopwatch.ElapsedMilliseconds}ms\n");

            Console.WriteLine("REDUCE PHASE");
            Console.WriteLine("─────────────────────────────────────────────────────────────────");
            var reduceStopwatch = Stopwatch.StartNew();
            
            var output = new Dictionary<string, string>();
            foreach (var kv in grouped)
            {
                string reduced = _reducer.Reduce(kv.Key, kv.Value);
                output[kv.Key] = reduced;
            }
            
            reduceStopwatch.Stop();
            Console.WriteLine($"Reduce phase complete: {output.Count:N0} final records in {reduceStopwatch.ElapsedMilliseconds}ms\n");

            // Write Output
            Console.WriteLine("WRITING OUTPUT");
            Console.WriteLine("─────────────────────────────────────────────────────────────────");
            var writeStopwatch = Stopwatch.StartNew();
            
            using (var writer = new StreamWriter(outputPath))
            {
                foreach (var kv in output)
                {
                    writer.WriteLine($"{kv.Key}\t{kv.Value}");
                }
            }
            
            writeStopwatch.Stop();
            Console.WriteLine($"Output written to: {outputPath}\n");

            totalStopwatch.Stop();

            Console.WriteLine("╔════════════════════════════════════════════════════════════════╗");
            Console.WriteLine("║                      EXECUTION SUMMARY                         ║");
            Console.WriteLine("╚════════════════════════════════════════════════════════════════╝");
            Console.WriteLine($"Total execution time:     {totalStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Map phase:              {mapStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Shuffle phase:          {shuffleStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Reduce phase:           {reduceStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"  Write phase:            {writeStopwatch.ElapsedMilliseconds}ms");
            Console.WriteLine($"Intermediate records:     {intermediateResults.Count:N0}");
            Console.WriteLine($"Final records:            {output.Count:N0}");
            Console.WriteLine($"Execution mode:           Sequential (single-threaded)");
            Console.WriteLine();
        }
    }
}
