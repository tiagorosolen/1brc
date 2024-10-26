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

            // This values without decimal points wont even show up in the 1brc data.. but why not keep it...
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
                File.Open(path, FileMode.Open, FileAccess.Read, FileShare.Read),
                null,
                0L,
                MemoryMappedFileAccess.Read,
                HandleInheritability.Inheritable,
                false);
        }
    }
}
