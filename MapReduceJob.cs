using System;
using System.Collections.Generic;
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

        public void ReadInput(string filePath)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"File not found: {filePath}");

            int lineNumber = 0;
            var intermediateResults = new List<KeyValuePair<string, string>>();

            Console.WriteLine("=== MAP PHASE ===");
            foreach (var line in File.ReadLines(filePath))
            {
                var mapResult = _mapper.Map(lineNumber.ToString(), line);

                foreach (var kv in mapResult)
                {
                    intermediateResults.Add(new KeyValuePair<string, string>(kv.Key, kv.Value));
                    Console.WriteLine($"Mapped: ({kv.Key}, {kv.Value})");
                }

                lineNumber++;
            }

            Console.WriteLine("\n=== SHUFFLE PHASE ===");
            var grouped = intermediateResults
                .GroupBy(kv => kv.Key)
                .ToDictionary(g => g.Key, g => g.Select(v => v.Value));

            foreach (var kv in grouped)
            {
                Console.WriteLine($"{kv.Key} -> [{string.Join(", ", kv.Value)}]");
            }

            
            Console.WriteLine("\n=== REDUCE PHASE ===");
            var output = new Dictionary<string, string>();
            foreach (var kv in grouped)
            {
                string reduced = _reducer.Reduce(kv.Key, kv.Value);
                output[kv.Key] = reduced;
                Console.WriteLine($"Reduced: ({kv.Key}, {reduced})");
            }

           
            Console.WriteLine("\n=== FINAL OUTPUT ===");
            foreach (var kv in output)
            {
                Console.WriteLine($"{kv.Key}: {kv.Value}");
            }
        }
    }
}
