using System.IO.MemoryMappedFiles;

namespace OneBrcUtilities
{
    public static class OneBrcUtility
    {
        public static double ParseDouble(byte[] buff, int v1, int v2)
        {
            // Largest format possible = -XX.X
            // '.' can only be at index 1, 2 or 3
            var len = v2;
            bool negative = (buff[v1] == 45);
            v1 = (negative ? v1 + 1 : v1);

            if (buff[v1 + 1] == 46) 
            {
                // X.X
                return ((buff[v1] - 0x30) + ((buff[v1 + 2] - 0x30) / 10.0)) * (negative ? -1 : 1);
            }
            if (buff[v1 + 2] == 46)
            {
                // XX.X
                return ((((buff[v1] - 0x30) * 10) + buff[v1 + 1] - 0x30) + ((buff[v1 + 3] - 0x30) / 10.0)) * (negative ? -1 : 1);
            }
            if(v1 + 1 == v2) 
            {
                // X
                return (buff[v1] - 0x30) * (negative ? -1 : 1);
            }
            if (v1+2 == v2)
            {
                // XX
                return (((buff[v1] - 0x30) * 10) + buff[v1 + 1] - 0x30) * (negative ? -1 : 1);
            }
            throw new ArgumentException("ParseDouble failed.");
        }

        public static MemoryMappedFile CreateMemoryMappedFile(string path)
        {
            return MemoryMappedFile.CreateFromFile(
                      //include a readonly shared stream
                      File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read),
                      //not mapping to a name
                      null,
                      //use the file's actual size
                      0L,
                      //read only access
                      MemoryMappedFileAccess.Read,
                      //adjust as needed
                      HandleInheritability.Inheritable,
                      //close the previously passed in stream when done
                      false);

        }
    }
}
