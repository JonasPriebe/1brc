// See https://aka.ms/new-console-template for more information

using _1brc.IO;

var start = DateTime.Now;
IO.ReadContents(new FileInfo("/home/bob/projects/1brc.data/measurements-1000000000.txt"));

var end = DateTime.Now;
Console.WriteLine("Run took '" + end.Subtract(start).TotalSeconds + "' seconds to finish.");