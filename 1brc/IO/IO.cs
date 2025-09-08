using System.Diagnostics.CodeAnalysis;
using System.IO.MemoryMappedFiles;
using System.Text;

namespace _1brc.IO;

public class IO
{
    [SuppressMessage("ReSharper.DPA", "DPA0000: DPA issues")]
    public static void ReadContents(FileInfo file)
    {
        if (file == null)
            throw new ArgumentException(nameof(file) + " cannot be empty!");
        file.Refresh();
        if (file.Exists == false)
            throw new FileNotFoundException("Couldn't find input file at: " + file.FullName);

        var fileLength = file.Length;

        using var mmf = MemoryMappedFile.CreateFromFile(file.FullName, FileMode.Open, null, 0, MemoryMappedFileAccess.Read);
        using var accessor = mmf.CreateViewAccessor(0, fileLength, MemoryMappedFileAccess.Read);
        var chunksize = 64000;
        var byteBuffer = new byte[chunksize+4];
        var charBuffer = new char[chunksize];
        var leftoverLength = 0;
        long lineCounter = 0;
        for (long offset = 0; offset < fileLength; offset += chunksize )
        {
            var bytesToRead = (int)Math.Min(chunksize, fileLength - offset);
            accessor.ReadArray(offset, byteBuffer, leftoverLength, bytesToRead);
            
            var totalBytes = leftoverLength + bytesToRead;
            ReadOnlySpan<byte> spanToDecode = byteBuffer.AsSpan(0, totalBytes);
            
            Decoder decoder = Encoding.UTF8.GetDecoder();
            decoder.Convert(spanToDecode.ToArray(), 0, spanToDecode.Length, charBuffer, 0, charBuffer.Length, false,
                out int bytesUsed, out int charsUsed, out bool completed);
            
            leftoverLength = totalBytes - bytesUsed;
            if (leftoverLength > 0)
                Buffer.BlockCopy(byteBuffer, bytesUsed, byteBuffer, 0, leftoverLength);
            
            int lineStart = 0;
            for (var i = 0; i < charsUsed; i++)
            {
                if (charBuffer[i] == '\n')
                {
                    ReadOnlySpan<char> lineSpan = charBuffer.AsSpan(lineStart, i - lineStart);
                    Console.WriteLine(lineSpan.ToString());
                    lineCounter++;
                    lineStart = i + 1;
                }
            }
            var partialLength = charsUsed - lineStart;
            if (partialLength > 0)
                Buffer.BlockCopy(charBuffer, lineStart * sizeof(char), charBuffer, 0, partialLength * sizeof(char));
        }
        if (leftoverLength > 0)
        {
            ReadOnlySpan<char> finalLine = charBuffer.AsSpan(0, leftoverLength);
            lineCounter++;
        }

        Console.WriteLine("Total lines: " + lineCounter);
    }
    
}