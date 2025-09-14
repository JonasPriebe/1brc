using System.Collections.Concurrent;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace _1brc.IO;

public class IO
{
    class Accumulator
    {
        public float mean = 0;
        public float min = float.MaxValue;
        public float max = float.MinValue;
        public int count = 0;
    }

    public static void ReadContents(FileInfo file)
    {
        if (file == null)
            throw new ArgumentException(nameof(file) + " cannot be empty!");
        file.Refresh();
        if (file.Exists == false)
            throw new FileNotFoundException("Couldn't find input file at: " + file.FullName);
        byte[] data = File.ReadAllBytes(file.FullName);
        // TODO: find solution, Memory<byte> only supports files up to 2gb 
        Memory<byte> memory = data;

        var maxThreads = Environment.ProcessorCount;
        Console.WriteLine("System has '" + maxThreads + " usable Processors.");

        var fileLength = file.Length;
        var chunkSize = (int)Math.Min(fileLength / maxThreads, Int32.MaxValue - 1);
        var workers = new Thread[maxThreads];
        
        var threadCounts = new int[maxThreads];
        var threadDicts = new Dictionary<string, Accumulator>[maxThreads];
        var stack = new ConcurrentQueue<Memory<byte>>();
        
        foreach (var slice in SplitByLines(memory, chunkSize))
            stack.Enqueue(slice);
        
        Console.WriteLine(stack.Count);
        
        for (var t = 0; t < maxThreads; t++)
        {
            var idx = t;
            workers[idx] = new Thread(() =>
            {
                while (stack.TryDequeue(out var memoryPart))
                {   threadDicts[idx] ??= new Dictionary<string, Accumulator>();
                    threadCounts[idx] += EntryParse(memoryPart, threadDicts[idx]);
                }
            });
            workers[idx].Start();
        }
        
        Console.WriteLine("All workers have been started.");
        foreach (var w in workers)
            w.Join();
        
        PrintSummary(threadCounts, threadDicts);
        for(var i = 0; i < workers.Length; i ++)
            Console.WriteLine("Thread " + i + " has read '" + threadCounts[i] + "' lines.");
        
    }

    static int EntryParse(Memory<byte> memory, Dictionary<string, Accumulator> threadDict)
    {
        var blockSize = 64 * 1024;
        var count = 0;

        var bytes = memory.Span;
        var utf8 = Encoding.UTF8;
        var decoder = utf8.GetDecoder();

        var byteIndex = 0;
        var charBuffer = new char[utf8.GetMaxCharCount(blockSize)];
        var carryLine = new List<char>();

        while (byteIndex < bytes.Length)
        {
            var bytesToRead = Math.Min(blockSize, bytes.Length - byteIndex);
            var decodedChars = decoder.GetChars(bytes.Slice(byteIndex, bytesToRead), charBuffer, false);

            var start = 0;
            for (var i = 0; i < decodedChars; i++)
            {
                if (charBuffer[i] == '\n')
                {
                    if (carryLine.Count > 0)
                    {
                        carryLine.AddRange(charBuffer.AsSpan(start, i - start));
                        ProcessLine(carryLine.ToArray(), threadDict);
                        carryLine.Clear();
                    }
                    else
                    {
                        ProcessLine(charBuffer.AsSpan(start, i - start), threadDict);
                    }
                    count++;
                    start = i + 1;
                }
            }

            if (start < decodedChars)
                carryLine.AddRange(charBuffer.AsSpan(start, decodedChars - start));

            byteIndex += bytesToRead;
        }

        if (carryLine.Count <= 0) 
            return count;
        
        ProcessLine(carryLine.ToArray(), threadDict);
        count++;
        
        return count;
    }

    // With naive block partitioning of the original memory <T> i ran into the issue of the blocks not ending on \n
    // this could be managed with overlapping memory<T> splits, but the more elegant approach is to end the blocks 
    // on \n by default, so we don't have to worry about boundaries, which makes the multithreaded scanning easier
    static Memory<byte>[] SplitByLines(Memory<byte> memory, int chunkSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);

        var chunks = new List<Memory<byte>>();
        var total = memory.Length;
        var start = 0;

        while (start < total)
        {
            var end = Math.Min(start + chunkSize, total);
            var scan = end;
            
            while (scan < total && memory.Span[scan] != (byte)'\n')
                scan++;

            if (scan < total)
                scan++;

            chunks.Add(memory.Slice(start, scan - start));
            start = scan;
        }
        return chunks.ToArray();
    }

    static void PrintSummary(int[] threadCounts, Dictionary<string, Accumulator>[] threadDicts)
    {
        var total = threadCounts.Sum();
        var globalDict = new Dictionary<string, Accumulator>();
        foreach (var dict in threadDicts)
        {
            foreach (var entry in dict)
            {
                var key = entry.Key;
                var acc = entry.Value;
                if (globalDict.TryGetValue(key, out var globalAcc) == false)
                {
                    globalDict[key] = new Accumulator()
                    {
                        min = acc.min,
                        max = acc.max,
                        mean = acc.mean,
                        count = acc.count,
                    };
                }
                else
                {
                    globalAcc.min = Math.Min(acc.min, globalAcc.min);
                    globalAcc.max = Math.Max(acc.max, globalAcc.max);
                    globalAcc.count += acc.count;
                    globalAcc.mean = (globalAcc.mean * globalAcc.count + acc.mean + acc.count) / globalAcc.count;
                }
            }
        }

        Console.WriteLine("total lines read after Join: " + total);
        foreach (var kvp in globalDict.OrderBy(x => x.Key))
        {
            Console.WriteLine(kvp.Key + ";" + kvp.Value.min + ";" + kvp.Value.mean + ";" + kvp.Value.max);
        }
    }

    static void ProcessLine(ReadOnlySpan<char> line, Dictionary<string, Accumulator> threadDict)
    {
        var seperatorIndex = line.IndexOf(';');
        if (seperatorIndex == -1)
        {
            Console.WriteLine("malformed line: " + line.ToString());
            return;
        }

        var keySpan = line.Slice(0, seperatorIndex);
        var valueSpan = line.Slice(seperatorIndex + 1);

        var key = string.Intern(keySpan.ToString());

        if (float.TryParse(valueSpan, out var value) == false)
        {
            Console.WriteLine("Unable to parse: '" + valueSpan.ToString() + "' into a valid float!");
            return;
        }
        CalculateValues(key, value, threadDict);
    }

    static void CalculateValues(string key, float value, Dictionary<string, Accumulator> threadDict)
    {
        if (threadDict.TryGetValue(key, out var inputValues) == false)
        {
            threadDict[key] = new Accumulator() { min = value, max = value, mean = value, count = 1 };
            return;
        }

        inputValues.min = Math.Min(value, inputValues.min);
        inputValues.max = Math.Max(value, inputValues.max);
        // https://constreference.wordpress.com/2019/11/29/incremental-means-and-variances/
        inputValues.mean += (value - inputValues.mean) / ++inputValues.count;
    }
}
