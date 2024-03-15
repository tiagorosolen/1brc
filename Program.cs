using System.Diagnostics;
using System.Globalization;
using System.Text;

internal class Program
{
    public struct Reading{
        public string name;
        public double value;
    }

    public class Station
    {
        public double avg;
        public double count;
        public double min;
        public double max;
    }

    public static List<Dictionary<string, Station>> allResults = new List<Dictionary<string, Station>>();
    public static Dictionary<int, (long, long)> mapOfBytePositions = new Dictionary<int, (long, long)>();
    public static long lines = 0;

    private static async Task Main(string[] args)
    {

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

        Console.WriteLine($"Initializing...");

        // Working variables
        Dictionary<string, Station> stations = new Dictionary<string, Station>();

        // Loading the file
        Stopwatch timer = new Stopwatch();
        timer.Start();
        Console.WriteLine($"Opening file...");

        int numOfCpus = 16;
        var options = new FileStreamOptions();
        options.BufferSize = 1;
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
        var linesPerCpu = lineToProcess / numOfCpus; //
        Task[] tasks = new Task[numOfCpus];
        while (cpuLoops < numOfCpus)
        {
            var startPoint = mapOfBytePositions[cpuLoops].Item1;
            var stopByte = mapOfBytePositions[cpuLoops].Item2;
            tasks[cpuLoops] = ProcessListByLine(cpuLoops, args[0], startPoint, stopByte);
            cpuLoops++;
        }

        // 
        Console.WriteLine($"Processing...");
        await Task.WhenAll(tasks);

        //  TODO -> Get the content together
        stations = IntegrateResults(allResults);        

        timer.Stop();

        Console.WriteLine(" ");
        Console.WriteLine($"File with {lines} registries was processed in {timer.Elapsed.TotalMilliseconds/1000}s ");

        var sortedDict = stations.OrderBy(pair => pair.Key).ToDictionary(pair => pair.Key, pair => pair.Value);
        using (StreamWriter outputFile = new StreamWriter("c:/temp/1B_unchecked.txt"))
        {
            foreach (var station in sortedDict)
            {
                double avg = (station.Value.avg / station.Value.count);
                outputFile.WriteLine($"{station.Key};{station.Value.min};{station.Value.max};{avg.ToString("0.##")}");
            }
        }
    }

    private static async Task<bool> ProcessListByReadLine(int cpu, string file, long startByte, long stopByte)
    {
        Dictionary<string, Station> stations = new Dictionary<string, Station>();

        await Task.Run(() =>
        {
            var data = File.OpenText(file);
            data.BaseStream.Position = startByte;
            long linesProcesssed = 0;
            long bytesProcessed = 0;
            var stringLine = "";
            while (bytesProcessed + startByte <= stopByte)
            {
                stringLine = data.ReadLine();
                if (stringLine != null)
                {
                    var point = stringLine.Split(';');
                    double value = double.Parse(point[1], CultureInfo.InvariantCulture);

                    if (stations.ContainsKey(point[0]))
                    {
                        if (stations[point[0]].min > value)
                            stations[point[0]].min = value;

                        if (stations[point[0]].max < value)
                            stations[point[0]].max = value;

                        stations[point[0]].avg += value;
                        stations[point[0]].count++;
                    }
                    else
                    {
                        var st = new Station();
                        st.min = value;
                        st.max = value;
                        st.avg = value;
                        st.count++;
                        stations.Add(point[0], st);
                    }
                    bytesProcessed = bytesProcessed + stringLine.Length + 1;
                    linesProcesssed++;
                }
                else
                {
                    Console.WriteLine($"CPU {cpu + 1} completed. Processed {linesProcesssed} lines");
                    break;
                }
            }
            Console.WriteLine($"CPU {cpu + 1} completed. Processed {linesProcesssed} lines");
            allResults.Add(stations);
            lines += linesProcesssed;
        });

        return true;
    }

    private static async Task<bool> ProcessListByLine(int cpu, string file, long startByte, long stopByte)
    {
        Dictionary<string, Station> stations = new Dictionary<string, Station>();
        
        await Task.Run(() =>
        {
            var options = new FileStreamOptions();
            options.BufferSize = 16000;
            var data = new FileStream(file, options);
            data.Position = startByte;
            
            byte[] buff = new byte[128];
            long bytesProcessed = 0;
            var linesProcesssed = 0;
            //string lineToBeread = "";
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

                //var point = lineToBeread.Split(';');
                var point = Encoding.UTF8.GetString(buff, 0, comma);
                double value = double.Parse(Encoding.UTF8.GetString(buff, comma + 1, i - comma-1), CultureInfo.InvariantCulture);

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
                    st.count++;
                    stations.Add(point, st);
                }
                linesProcesssed++;
                comma = 0;
                //lineToBeread = "";
            }
            //Console.WriteLine($"CPU {cpu+1} completed. Processed {linesProcesssed} lines");
            allResults.Add(stations);
            lines += linesProcesssed;
        });

        return true;
    }
    private static Dictionary<string, Station> IntegrateResults(List<Dictionary<string, Station>> allResults)
    {
        Dictionary<string, Station> stations = new Dictionary<string, Station>();

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

    /*
    private static async Task<bool> ProcessList(int cpu, string file, long startByte, long stopByte )
    {
        await Task.Run(() =>
        {
            Dictionary<string, Station> stations = new Dictionary<string, Station>();            

            var options = new FileStreamOptions();
            options.BufferSize = 1;
            var data = new FileStream(file, options);
            data.Position = startByte;

            string line = string.Empty;
            long bytesProcessed = 0;

            bool printDebug = true;
            int internalLines = 0;

            stopByte = (stopByte - startByte);
            byte[] theLine = new byte[100];
            var theLineCounter = 0;
            for (int i=0; bytesProcessed < stopByte; bytesProcessed++)
            { 
                byte[] b = new byte[1];
                data.Read(b, 0, b.Length);
                theLine[theLineCounter] = b[0];
                if (b[0] == '\n')
                {
                    line = System.Text.Encoding.UTF8.GetString(theLine,0, theLineCounter);
                    theLineCounter = 0;
                    if (printDebug)
                    {
                        Console.WriteLine(" ");
                        Console.WriteLine($"CPU {cpu} from {startByte} to {stopByte}. First line: {line}");
                        printDebug = false;
                    }

                    var point = line.Split(';');
                    var reading = new Reading() { name = point[0], value = double.Parse(point[1]) };

                    if (stations.ContainsKey(reading.name))
                    {
                        if (stations[reading.name].min > reading.value)
                            stations[reading.name].min = reading.value;

                        if (stations[reading.name].max < reading.value)
                            stations[reading.name].max = reading.value;

                        stations[reading.name].avg = (stations[reading.name].avg + reading.value) / 2;
                    }
                    else
                    {
                        stations.Add(reading.name, new Station() { avg = reading.value, max = reading.value, min = reading.value });
                        
                    }
                    line = string.Empty;
                    internalLines++;
                }
                else
                {
                    theLineCounter++;
                }
                
            }

            lines += internalLines;
            Console.WriteLine($"Cpu '{cpu}' done. Lines read: {internalLines}");

            allResults.Add(stations);
        });

        return true;
    }*/


    /*
    readonly static FieldInfo charPosField = typeof(StreamReader).GetField("_charPos", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
    //readonly static FieldInfo charLenField = typeof(StreamReader).GetField("charLen", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);
    //readonly static FieldInfo charBufferField = typeof(StreamReader).GetField("_charBuffer", BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.DeclaredOnly);

    public static long ActualPosition(StreamReader reader)
    {
        //var charBuffer = (char[])charBufferField.GetValue(reader);
        //var charLen = (int)charLenField.GetValue(reader);
        var charPos = (int)charPosField.GetValue(reader);

        return charPos;// reader.BaseStream.Position - reader.CurrentEncoding.GetByteCount(charBuffer, charPos, charLen - charPos);
    }*/
}