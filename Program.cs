// See https://aka.ms/new-console-template for more information

using mapreduce;
using MapReduce;

Console.WriteLine("Hello, World!");

var mapper = new Mapper();
var reducer = new Reducer();

var job = new MapReduceJob(mapper, reducer);
job.ReadInput("input.txt");