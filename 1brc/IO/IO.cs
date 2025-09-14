using System.Collections.Concurrent;
using System.Formats.Tar;
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
        using var mmf = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.Open, null, file.Length, MemoryMappedFileAccess.Read);

        var maxThreads = Environment.ProcessorCount;
        Console.WriteLine("System has '" + maxThreads + " usable Processors.");

        var fileLength = file.Length;
        var chunkSize = (int)Math.Min(fileLength / maxThreads, Int32.MaxValue - 1);
        var workers = new Thread[maxThreads];
        
        var threadCounts = new int[maxThreads];
        var threadDicts = new Dictionary<string, Accumulator>[maxThreads];
        for (int i = 0; i < threadDicts.Length; i++)
        {
            threadDicts[i] = new Dictionary<string, Accumulator>();
        }
        var stack = new ConcurrentQueue<MemoryMappedViewAccessor>();
        
        foreach (var slice in SplitByLines(mmf, chunkSize, file.Length))
            stack.Enqueue(slice);
        
        Console.WriteLine("workitems in stack: "+stack.Count);
        
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
        var sum = 0;
        for (var i = 0; i < workers.Length; i++)
        {
            Console.WriteLine("Thread " + i + " has read '" + threadCounts[i] + "' lines.");
            sum += threadCounts[i];
        }
        Console.WriteLine("Total lines read: " + sum);
        
    }

    static int EntryParse(MemoryMappedViewAccessor memory, Dictionary<string, Accumulator> threadDict)
    {
 
    }

    // With naive block partitioning of the original memory <T> i ran into the issue of the blocks not ending on \n
    // this could be managed with overlapping memory<T> splits, but the more elegant approach is to end the blocks 
    // on \n by default, so we don't have to worry about boundaries, which makes the multithreaded scanning easier
    static MemoryMappedViewAccessor[] SplitByLines(MemoryMappedFile memory, int chunkSize, long total)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(chunkSize);

        var chunks = new List<MemoryMappedViewAccessor>();
        long start = 0;

        while (start < total)
        {
            long end = Math.Min(start + chunkSize, total);
            int scanSize = (int)Math.Min(1000, total - end);

            byte[] buffer = new byte[scanSize];
            if (scanSize > 0)
            {
                using var accessor = memory.CreateViewAccessor(end, scanSize, MemoryMappedFileAccess.Read);
                accessor.ReadArray(0, buffer, 0, scanSize);
            }
            
            int index = Array.IndexOf(buffer, (byte)'\n');
            int size = (index != -1) ? (int)(end - start + index + 1) : (int)(end - start + scanSize);

            var completeView = memory.CreateViewAccessor(start, size, MemoryMappedFileAccess.Read);
            chunks.Add(completeView);

            start += size;
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
