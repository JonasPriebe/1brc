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

        using var mmf = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        
        var maxThreads = Environment.ProcessorCount;
        Console.WriteLine("System has '" + maxThreads + " usable Processors.");
        
        var fileLength = file.Length;
        var parts = fileLength / maxThreads;
        var threads = new Thread[maxThreads];
        var threadCounts = new int[maxThreads];
        var threadDicts = new Dictionary<string, Accumulator>[maxThreads];
        
        for (var t = 0; t < maxThreads; t++)
        {
            var idx = t;
            var start = parts * idx;
            var length = idx == maxThreads - 1 ? fileLength - start : parts;
            
            var accessor = mmf.CreateViewAccessor(start, length, MemoryMappedFileAccess.Read);
            
            threads[t] = new Thread(() =>
            {
                var threadRes = EntryParse(length, accessor, threadDicts[idx] = new Dictionary<string, Accumulator>());
                threadCounts[idx] = threadRes;
            });
            threads[t].Start();
            Console.WriteLine("Thread started with idx: " + idx);
        }

        for (var index = 0; index < threads.Length; index++)
        {
            threads[index].Join();
            Console.WriteLine("Thread " + index + " has read '" + threadCounts[index] + "' lines.");
        }

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
            Console.WriteLine(kvp.Key+";"+ kvp.Value.min + ";" + kvp.Value.mean + ";" + kvp.Value.max);
        }
    }

    static int EntryParse(long accessorLength, MemoryMappedViewAccessor accessor, Dictionary<string,Accumulator> threadDict)
    {
        var chunkSize = 64000;
        var byteBuffer = new byte[chunkSize + 4];
        var charBuffer = new char[chunkSize];
        var leftoverLength = 0;
        int leftoverChars = 0;
        var lineCounter = 0;
        var decoder = Encoding.UTF8.GetDecoder();
        for (long offset = 0; offset < accessorLength; offset += chunkSize)
        {
            var bytesToRead = (int)Math.Min(chunkSize, accessorLength - offset);
            accessor.ReadArray(offset, byteBuffer, leftoverLength, bytesToRead);

            var totalBytes = leftoverLength + bytesToRead;
            ReadOnlySpan<byte> spanToDecode = byteBuffer.AsSpan(0, totalBytes);

            decoder.Convert(byteBuffer, 0, totalBytes, charBuffer, leftoverChars, charBuffer.Length - leftoverChars, false,
                out var bytesUsed, out var charsUsed, out var completed);

            leftoverLength = totalBytes - bytesUsed;
            if (leftoverLength > 0)
                Buffer.BlockCopy(byteBuffer, bytesUsed, byteBuffer, 0, leftoverLength);

            var lineStart = 0;
            for (var i = 0; i < leftoverChars + charsUsed; i++)
            {
                if (charBuffer[i] != '\n') continue;
                // TODO: Add Processing, min, max, mean
                ReadOnlySpan<char> lineSpan = charBuffer.AsSpan(lineStart, i - lineStart).Trim();
                if(lineSpan.IsEmpty == false)
                    ProcessLine(lineSpan, threadDict);
                lineCounter++;
                lineStart = i + 1;
            }

            leftoverChars = leftoverChars + charsUsed - lineStart;
            if (leftoverChars > 0)
                Buffer.BlockCopy(charBuffer, lineStart * sizeof(char), charBuffer, 0, leftoverChars * sizeof(char));

        }

        if (leftoverLength <= 0) return lineCounter;
        // TODO: Add Processing, min, max, mean
        ReadOnlySpan<char> finalLine = charBuffer.AsSpan(0, leftoverChars);
        if (finalLine.Contains('\n'))
            ProcessLine(finalLine, threadDict);
        lineCounter++;

        return lineCounter;
    }

    static void ProcessLine(ReadOnlySpan<char> line, Dictionary<string, Accumulator> threadDict)
    {
        int seperatorIndex = line.IndexOf(';');
        if (seperatorIndex == -1)
        {
            Console.WriteLine("malformed line: " + line.ToString());
            return;
        }
        
        ReadOnlySpan<char> keySpan = line.Slice(0, seperatorIndex);
        ReadOnlySpan<char> valueSpan = line.Slice(seperatorIndex + 1);

        string key = string.Intern(keySpan.ToString());

        if (float.TryParse(valueSpan, out var value) == false)
        {
            Console.WriteLine("Unable to parse: '" + valueSpan.ToString() + "' into a valid float!");
            return;
        }
        CalculateValues(key,value, threadDict);
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