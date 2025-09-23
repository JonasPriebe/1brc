using System.Collections.Concurrent;
using System.Formats.Tar;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Text;

namespace _1brc.IO;

public class IO
{
    class Accumulator
    {
        public float Mean = 0;
        public float Min = float.MaxValue;
        public float Max = float.MinValue;
        public int Count = 0;
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
        var count = 0;

        // Read entire accessor into a single byte array
        var bytes = new byte[memory.Capacity];
        memory.ReadArray(0, bytes, 0, (int)memory.Capacity);

        var utf8 = Encoding.UTF8;
        var decoder = utf8.GetDecoder();

        // Use a smaller char buffer for incremental decoding
        int blockSize = 4 * 1024 * 1024; // 4 MB blocks
        var charBuffer = new char[utf8.GetMaxCharCount(blockSize)];

        var carryLine = new List<char>();
        int byteIndex = 0;
        var stringBuffer = new StringBuilder(capacity:100);
        while (byteIndex < bytes.Length)
        {
            int bytesToDecode = Math.Min(blockSize, bytes.Length - byteIndex);

            decoder.Convert(
                bytes, byteIndex, bytesToDecode,
                charBuffer, 0, charBuffer.Length,
                flush: false,
                out int bytesUsed, out int charsUsed, out bool completed
            );

            // Process characters
            int lineStart = 0;
            for (int i = 0; i < charsUsed; i++)
            {
                if (charBuffer[i] == '\n')
                {
                    if (carryLine.Count > 0)
                    {
                        carryLine.AddRange(charBuffer.AsSpan(lineStart, i - lineStart));
                        ProcessLine(CollectionsMarshal.AsSpan(carryLine), threadDict, stringBuffer);
                        carryLine.Clear();
                    }
                    else
                    {
                        ProcessLine(charBuffer.AsSpan(lineStart, i - lineStart), threadDict, stringBuffer);
                    }
                    count++;
                    lineStart = i + 1;
                }
            }

            // Save leftover characters that did not end with '\n'
            if (lineStart < charsUsed)
                carryLine.AddRange(charBuffer.AsSpan(lineStart, charsUsed - lineStart));

            byteIndex += bytesUsed;
        }

        // Process any remaining characters
        if (carryLine.Count > 0)
        {
            ProcessLine(carryLine.ToArray(), threadDict, stringBuffer);
            count++;
        }

        return count;
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
            var end = Math.Min(start + chunkSize, total);
            var scanSize = (int)Math.Min(1000, total - end);

            var buffer = new byte[scanSize];
            if (scanSize > 0)
            {
                using var accessor = memory.CreateViewAccessor(end, scanSize, MemoryMappedFileAccess.Read);
                accessor.ReadArray(0, buffer, 0, scanSize);
            }

            var index = Array.IndexOf(buffer, (byte)'\n');
            var size = index != -1 ? (int)(end - start + index + 1) : (int)(end - start + scanSize);

            var completeView = memory.CreateViewAccessor(start, size, MemoryMappedFileAccess.Read);
            chunks.Add(completeView);

            start += size;
        }

        return chunks.ToArray();
    }


    private static void PrintSummary(int[] threadCounts, Dictionary<string, Accumulator>[] threadDicts)
    {
        var total = threadCounts.Sum();
        var globalDict = new Dictionary<string, Accumulator>();
        foreach (var dict in threadDicts)
        foreach (var entry in dict)
        {
            var key = entry.Key;
            var acc = entry.Value;
            if (globalDict.TryGetValue(key, out var globalAcc) == false)
            {
                globalDict[key] = new Accumulator()
                {
                    Min = acc.Min,
                    Max = acc.Max,
                    Mean = acc.Mean,
                    Count = acc.Count
                };
            }
            else
            {
                globalAcc.Min = Math.Min(acc.Min, globalAcc.Min);
                globalAcc.Max = Math.Max(acc.Max, globalAcc.Max);
                globalAcc.Count += acc.Count;
                globalAcc.Mean = (globalAcc.Mean * globalAcc.Count + acc.Mean + acc.Count) / globalAcc.Count;
            }
        }

        Console.WriteLine("total lines read after Join: " + total);
        foreach (var kvp in globalDict.OrderBy(x => x.Key))
            Console.WriteLine(kvp.Key + ";" + kvp.Value.Min + ";" + kvp.Value.Mean + ";" + kvp.Value.Max);
    }

    private static void ProcessLine(ReadOnlySpan<char> line, Dictionary<string, Accumulator> threadDict, StringBuilder stringBuffer)
    {
        var seperatorIndex = line.IndexOf(';');
        if (seperatorIndex == -1)
        {
            Console.WriteLine("malformed line: " + line.ToString());
            return;
        }

        var keySpan = line.Slice(0, seperatorIndex);
        var valueSpan = line.Slice(seperatorIndex + 1);

        stringBuffer.Clear();
        stringBuffer.Insert(0,keySpan);
        //stringBuffer = new string(keySpan);

        if (float.TryParse(valueSpan, out var value) == false)
        {
            Console.WriteLine("Unable to parse: '" + valueSpan.ToString() + "' into a valid float!");
            return;
        }
        CalculateValues(stringBuffer, value, threadDict);
    }

    static void CalculateValues(StringBuilder key, float value, Dictionary<string, Accumulator> threadDict)
    {
        if (threadDict.TryGetValue(key.ToString(), out var inputValues) == false)
        {
            threadDict[key.ToString()] = new Accumulator() { Min = value, Max = value, Mean = value, Count = 1 };
            return;
        }

        inputValues.Min = Math.Min(value, inputValues.Min);
        inputValues.Max = Math.Max(value, inputValues.Max);
        // https://constreference.wordpress.com/2019/11/29/incremental-means-and-variances/
        inputValues.Mean += (value - inputValues.Mean) / ++inputValues.Count;
    }
}
