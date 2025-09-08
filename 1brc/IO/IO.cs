using System.IO.MemoryMappedFiles;
using System.Text;

namespace _1brc.IO;

public class IO
{
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
        
        for (var t = 0; t < maxThreads; t++)
        {
            var idx = t;
            var start = parts * idx;
            var length = idx == maxThreads - 1 ? fileLength - start : parts;
            
            var accessor = mmf.CreateViewAccessor(start, length, MemoryMappedFileAccess.Read);
            
            threads[t] = new Thread(() =>
            {
                var threadRes = EntryParse(length, accessor);
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
        Console.WriteLine("total lines read after Join: " + total);
    }

    private static int EntryParse(long accessorLength, MemoryMappedViewAccessor accessor)
    {
        var chunkSize = 64000;
        var byteBuffer = new byte[chunkSize + 4];
        var charBuffer = new char[chunkSize];
        var leftoverLength = 0;
        var lineCounter = 0;
        for (long offset = 0; offset < accessorLength; offset += chunkSize)
        {
            var bytesToRead = (int)Math.Min(chunkSize, accessorLength - offset);
            accessor.ReadArray(offset, byteBuffer, leftoverLength, bytesToRead);

            var totalBytes = leftoverLength + bytesToRead;
            ReadOnlySpan<byte> spanToDecode = byteBuffer.AsSpan(0, totalBytes);

            var decoder = Encoding.UTF8.GetDecoder();
            decoder.Convert(spanToDecode.ToArray(), 0, spanToDecode.Length, charBuffer, 0, charBuffer.Length, false,
                out var bytesUsed, out var charsUsed, out var completed);

            leftoverLength = totalBytes - bytesUsed;
            if (leftoverLength > 0)
                Buffer.BlockCopy(byteBuffer, bytesUsed, byteBuffer, 0, leftoverLength);

            var lineStart = 0;
            for (var i = 0; i < charsUsed; i++)
            {
                if (charBuffer[i] != '\n') continue;
                // TODO: Add Processing, min, max, mean
                ReadOnlySpan<char> lineSpan = charBuffer.AsSpan(lineStart, i - lineStart);
                lineCounter++;
                lineStart = i + 1;
            }

            var partialLength = charsUsed - lineStart;
            if (partialLength > 0)
                Buffer.BlockCopy(charBuffer, lineStart * sizeof(char), charBuffer, 0, partialLength * sizeof(char));
        }

        if (leftoverLength <= 0) return lineCounter;
        // TODO: Add Processing, min, max, mean
        ReadOnlySpan<char> finalLine = charBuffer.AsSpan(0, leftoverLength);
        lineCounter++;

        return lineCounter;
    }
}