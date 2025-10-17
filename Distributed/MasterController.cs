using Microsoft.AspNetCore.Mvc;
using MapReduce.Distributed.Models;

namespace MapReduce.Distributed
{
    [ApiController]
    [Route("api/master")]
    public class MasterController : ControllerBase
    {
        private static MasterNode? _masterNode;

        public static void Initialize(MasterNode masterNode)
        {
            _masterNode = masterNode;
        }

        [HttpPost("request-task")]
        public ActionResult<MapReduceTask?> RequestTask([FromBody] TaskRequest request)
        {
            if (_masterNode == null)
                return BadRequest("Master node not initialized");

            var task = _masterNode.RequestTask(request);
            
            if (task == null)
                return NoContent();

            return Ok(task);
        }

        [HttpPost("submit-task")]
        public ActionResult SubmitTask([FromBody] TaskSubmission submission)
        {
            if (_masterNode == null)
                return BadRequest("Master node not initialized");

            _masterNode.SubmitTaskResult(submission);
            return Ok();
        }

        [HttpPost("heartbeat")]
        public ActionResult Heartbeat([FromBody] WorkerHeartbeat heartbeat)
        {
            if (_masterNode == null)
                return BadRequest("Master node not initialized");

            _masterNode.ProcessHeartbeat(heartbeat);
            return Ok();
        }

        [HttpGet("status")]
        public ActionResult<JobStatus> GetStatus()
        {
            if (_masterNode == null)
                return BadRequest("Master node not initialized");

            var status = _masterNode.GetJobStatus();
            Console.WriteLine($"\n[Status Check] Tasks: {status.CompletedTasks}/{status.TotalTasks} | Running: {status.RunningTasks} | Failed: {status.FailedTasks} | Complete: {status.IsComplete}");
            
            return Ok(status);
        }
    }
}

