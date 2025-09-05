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
            var lines= chunk.Split("\n");
            for (var j = 0; j < lines.Length - 1; j++)
            {
                lineCounter++;
                Console.WriteLine("line: " + lineCounter +", Content: " + lines[j] );
            }
            leftover = lines[^1];
        }
        if(string.IsNullOrEmpty(leftover) == false)
            Console.WriteLine("line: " + ++lineCounter +", Content: " + leftover );
    }
    
}