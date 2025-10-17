using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace MapReduce
{
    public class InputSplitter
    {
        private readonly int _chunkSizeInBytes;

        public InputSplitter(int chunkSizeInMB = 16)
        {
            _chunkSizeInBytes = chunkSizeInMB * 1024 * 1024;
        }

        public List<InputChunk> SplitFile(string filePath)
        {
            if (!File.Exists(filePath))
                throw new FileNotFoundException($"File not found: {filePath}");

            var chunks = new List<InputChunk>();
            var currentChunk = new List<string>();
            int currentChunkSize = 0;
            int chunkId = 0;
            int lineNumber = 0;

            foreach (var line in File.ReadLines(filePath))
            {
                int lineSize = System.Text.Encoding.UTF8.GetByteCount(line);

                // If adding this line would exceed chunk size and we have content, start new chunk
                if (currentChunkSize + lineSize > _chunkSizeInBytes && currentChunk.Count > 0)
                {
                    chunks.Add(new InputChunk
                    {
                        ChunkId = chunkId,
                        Lines = currentChunk.ToList(),
                        StartLineNumber = lineNumber - currentChunk.Count,
                        EndLineNumber = lineNumber - 1
                    });

                    chunkId++;
                    currentChunk.Clear();
                    currentChunkSize = 0;
                }

                currentChunk.Add(line);
                currentChunkSize += lineSize;
                lineNumber++;
            }

            // Add the last chunk if it has content
            if (currentChunk.Count > 0)
            {
                chunks.Add(new InputChunk
                {
                    ChunkId = chunkId,
                    Lines = currentChunk.ToList(),
                    StartLineNumber = lineNumber - currentChunk.Count,
                    EndLineNumber = lineNumber - 1
                });
            }

            return chunks;
        }
    }

    public class InputChunk
    {
        public int ChunkId { get; set; }
        public List<string> Lines { get; set; } = new List<string>();
        public int StartLineNumber { get; set; }
        public int EndLineNumber { get; set; }

        public override string ToString()
        {
            return $"Chunk {ChunkId}: Lines {StartLineNumber}-{EndLineNumber} ({Lines.Count} lines)";
        }
    }
}

