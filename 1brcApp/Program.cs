using OneBrcUtilities;
using System.Diagnostics;
using System.Globalization;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Text;

internal class Program
{
    public class Station
    {
        public double avg;
        public double count;
        public double min;
        public double max;
        public string name;
    }

    public static List<Dictionary<int, Station>> allResults = new List<Dictionary<int, Station>>();
    public static Dictionary<int, (long, long)> mapOfBytePositions = new Dictionary<int, (long, long)>();
    public static long lines = 0;

    private static async Task Main(string[] args)
    {
        CultureInfo cultureInfo = CultureInfo.CreateSpecificCulture("en-US");
        Thread.CurrentThread.CurrentCulture = cultureInfo;

        var lineToProcess = 0;
        if (args.Length == 0)
        {
            Console.WriteLine($"Please insert a file name to be processed.");
            Environment.Exit(0);
        }

        if (!File.Exists(args[0]))
        {
            Console.WriteLine($"The file {args[0]} is invalid.");
            Environment.Exit(0);
        }

        int numOfCpus = Environment.ProcessorCount;
        if (args.Length == 2)
        {
            int.TryParse(args[1], out numOfCpus);
        }

        Console.WriteLine($"Initializing...");

        // Working variables        
        Dictionary<int, Station> stations = new Dictionary<int, Station>();
        Stopwatch timer = new Stopwatch();
        var options = new FileStreamOptions();
        options.BufferSize = 1;

        // Loading the file
        timer.Start();
        Console.WriteLine($"Opening file...");

        var baseFile = new FileStream(args[0], options);
        var totalBytes = baseFile.Length;
        int bytesPerCpu = (int)(totalBytes / numOfCpus);

        // split the bytes in <processors> blocks        
        long startOfBlock = 0;
        long endOfBlock = (int)bytesPerCpu;
        int stepsToBreak = 0;
        baseFile.Position = endOfBlock; // starts at the end        

        for (int i = 0; i < numOfCpus - 1; i++)
        {
            do
            {
                byte[] b = new byte[1];
                baseFile.Read(b, 0, b.Length);
                if (b[0] != '\n')
                {
                    stepsToBreak++;
                }
                else
                {
                    // found the \n. Let's update the endOfBlock
                    endOfBlock = endOfBlock + stepsToBreak;

                    mapOfBytePositions.Add(i, (startOfBlock, endOfBlock));
                    startOfBlock = endOfBlock + 1;
                    endOfBlock = startOfBlock + bytesPerCpu;
                    baseFile.Position = endOfBlock; // next block starts at the end of next block
                    stepsToBreak = 0;
                    break;
                }
            } while (true);
        }

        // the last one get's the rest
        mapOfBytePositions.Add(numOfCpus - 1, (startOfBlock, totalBytes));
        List<List<string>> listOfallBlocks = new List<List<string>>();

        int cpuLoops = 0;
        var linesPerCpu = lineToProcess / numOfCpus;
        Task[] tasks = new Task[numOfCpus];

        var useMMF = true;
        if (useMMF)
        {
            MemoryMappedFile mmf = OneBrcUtility.CreateMemoryMappedFile(args[0]);
            while (cpuLoops < numOfCpus)
            {
                var startPoint = mapOfBytePositions[cpuLoops].Item1;
                var stopByte = mapOfBytePositions[cpuLoops].Item2;
                tasks[cpuLoops] = ProcessListByMmf(mmf, startPoint, stopByte);
                cpuLoops++;
            }
        }
        else
        {
            while (cpuLoops < numOfCpus)
            {
                var startPoint = mapOfBytePositions[cpuLoops].Item1;
                var stopByte = mapOfBytePositions[cpuLoops].Item2;
                tasks[cpuLoops] = ProcessListByLine(cpuLoops, args[0], startPoint, stopByte);
                cpuLoops++;
            }
        }

        // 
        Console.WriteLine($"Processing...");
        await Task.WhenAll(tasks);

        //
        stations = IntegrateResults(allResults);

        timer.Stop();

        Console.WriteLine(" ");
        Console.WriteLine($"File with {lines} registries was processed in {(timer.Elapsed.TotalMilliseconds / 1000).ToString("0.##")}s ");

        var sortedDict = stations.OrderBy(pair => pair.Value.name).ToDictionary(pair => pair.Key, pair => pair.Value);
        using (StreamWriter outputFile = new StreamWriter("c:/temp/1B_unchecked.txt"))
        {
            foreach (var station in sortedDict)
            {
                double avg = (station.Value.avg / station.Value.count);
                outputFile.WriteLine($"{station.Value.name};{station.Value.min};{station.Value.max};{avg.ToString("0.##")}");
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static async Task<bool> ProcessListByMmf(MemoryMappedFile mmf, long startByte, long stopByte)
    {
        Dictionary<int, Station> stations = new Dictionary<int, Station>();

        await Task.Run(() =>
        {
            var size = stopByte - startByte;
            using (var stream = mmf.CreateViewStream(startByte, size, MemoryMappedFileAccess.Read))
            {

                byte[] buff = new byte[48];
                long bytesProcessed = 0;
                var linesProcesssed = 0;
                int comma = 0;
                int i = 0;
                while (bytesProcessed + startByte < stopByte)
                {
                    stream.Read(buff, 0, 48);
                    for (i = 0; i < 48; i++)
                    {
                        bytesProcessed++;
                        if (buff[i] != 10)
                        {
                            if (buff[i] == ';')
                                comma = i;
                        }
                        else
                        {
                            break;
                        }
                    }
                    stream.Position = bytesProcessed;

                    var pointName = Encoding.UTF8.GetString(buff, 0, comma);
                    //var value = double.Parse(Encoding.UTF8.GetString(buff, comma + 1, i - comma - 1));
                    double value = OneBrcUtility.ParseDouble(buff, comma + 1, i - comma - 1);
                    var point = pointName.GetHashCode();

                    if (stations.ContainsKey(point))
                    {
                        if (stations[point].min > value)
                            stations[point].min = value;

                        if (stations[point].max < value)
                            stations[point].max = value;

                        stations[point].avg += value;
                        stations[point].count++;
                    }
                    else
                    {
                        var st = new Station();
                        st.min = value;
                        st.max = value;
                        st.avg = value;
                        st.name = pointName;
                        st.count++;
                        stations.Add(point, st);
                    }
                    linesProcesssed++;
                    comma = 0;
                    buff = new byte[48];
                }
                allResults.Add(stations);
                lines += linesProcesssed;
            }
        });

        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveOptimization)]
    private static async Task<bool> ProcessListByLine(int cpu, string file, long startByte, long stopByte)
    {
        Dictionary<int, Station> stations = new Dictionary<int, Station>();

        await Task.Run(() =>
        {
            var options = new FileStreamOptions();
            options.BufferSize = 1024;
            var data = new FileStream(file, options);
            data.Position = startByte;

            byte[] buff = new byte[128];
            long bytesProcessed = 0;
            var linesProcesssed = 0;
            int comma = 0;
            int i = 0;
            while (bytesProcessed + startByte < stopByte)
            {
                for (i = 0; i < 128; i++)
                {
                    var b = data.ReadByte();
                    bytesProcessed++;
                    if (b != 10 && b != -1)
                    {
                        buff[i] = (byte)b;
                        if (b == ';')
                            comma = i;
                    }
                    else
                    {
                        break;
                    }
                }

                var pointName = Encoding.UTF8.GetString(buff, 0, comma);
                //double value = double.Parse(Encoding.UTF8.GetString(buff, comma + 1, i - comma - 1), CultureInfo.InvariantCulture);
                double value = OneBrcUtility.ParseDouble(buff, comma + 1, i - comma - 1);
                var point = pointName.GetHashCode();

                if (stations.ContainsKey(point))
                {
                    if (stations[point].min > value)
                        stations[point].min = value;

                    if (stations[point].max < value)
                        stations[point].max = value;

                    stations[point].avg += value;
                    stations[point].count++;
                }
                else
                {
                    var st = new Station();
                    st.min = value;
                    st.max = value;
                    st.avg = value;
                    st.name = pointName;
                    st.count++;
                    stations.Add(point, st);
                }
                linesProcesssed++;
                comma = 0;
            }
            allResults.Add(stations);
            lines += linesProcesssed;
        });

        return true;
    }

    private static Dictionary<int, Station> IntegrateResults(List<Dictionary<int, Station>> allResults)
    {
        Dictionary<int, Station> stations = new Dictionary<int, Station>();

        foreach (var dictOfResults in allResults)
        {
            foreach (var listOfResults in dictOfResults)
            {
                if (stations.ContainsKey(listOfResults.Key))
                {
                    if (stations[listOfResults.Key].min > listOfResults.Value.min)
                        stations[listOfResults.Key].min = listOfResults.Value.min;

                    if (stations[listOfResults.Key].max < listOfResults.Value.max)
                        stations[listOfResults.Key].max = listOfResults.Value.max;

                    stations[listOfResults.Key].avg = stations[listOfResults.Key].avg + listOfResults.Value.avg;
                    stations[listOfResults.Key].count += listOfResults.Value.count;
                }
                else
                {
                    stations.Add(listOfResults.Key, listOfResults.Value);
                }
            }
        }

        return stations;
    }
}