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
        long lineCounter = 0;
        byte[] buffer = new byte[64000];
        var leftover = "";
        for (long i = 0; i < fileLength; i += 64000 )
        {
            var bytesToRead = (int)Math.Min(buffer.Length, fileLength - i);
            accessor.ReadArray(i, buffer, 0, bytesToRead);
            var chunk = leftover + Encoding.UTF8.GetString(buffer, 0, bytesToRead);
            using (var reader = new StringReader(chunk))
            {
                string prevLine = null;
                string? line;
                while ((line = reader.ReadLine())!= null)
                {
                    prevLine = line;
                    lineCounter++;
                }

                if (chunk.EndsWith("\n") || prevLine == null) continue;
                leftover = prevLine;
                lineCounter--;
            }
        }
        if(string.IsNullOrEmpty(leftover) == false)
            Console.WriteLine("line: " + lineCounter  );
    }
    
}