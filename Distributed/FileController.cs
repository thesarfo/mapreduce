using Microsoft.AspNetCore.Mvc;
using System.IO;

namespace MapReduce.Distributed
{
    [ApiController]
    [Route("files")]
    public class FileController : ControllerBase
    {
        [HttpGet("{jobId}/{taskDir}/{fileName}")]
        public IActionResult GetFile(string jobId, string taskDir, string fileName)
        {
            var basePath = Path.Combine(Path.GetTempPath(), "mapreduce");
            var filePath = Path.Combine(basePath, taskDir, fileName);

            Console.WriteLine($"[FileServer] Request for: {filePath}");

            if (!System.IO.File.Exists(filePath))
            {
                Console.WriteLine($"[FileServer] File NOT FOUND: {filePath}");
                return NotFound($"File not found: {filePath}");
            }

            Console.WriteLine($"[FileServer] Serving file: {filePath}");
            var fileBytes = System.IO.File.ReadAllBytes(filePath);
            return File(fileBytes, "text/plain");
        }
    }
}

