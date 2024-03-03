using System.Diagnostics;
using System.IO;
using static Program;

internal class Program
{
    public struct Reading{
        public string name;
        public double value;
    }

    public class Station
    {
        public double avg;
        public double min;
        public double max;
    }

    public static List<Dictionary<string, Station>> allResults = new List<Dictionary<string, Station>>();
    public static Dictionary<int, (long, long)> mapOfBytePositions = new Dictionary<int, (long, long)>();

    private static async Task Main(string[] args)
    {
        var lines = 0;
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

        if (!int.TryParse(args[1], out lineToProcess))
        {
            Console.WriteLine($"Please inform the number of lines to be processed.");
            Environment.Exit(0);
        }

        Console.WriteLine($"Initializing...");

        // Working variables
        Dictionary<string, Station> stations = new Dictionary<string, Station>();

        // Loading the file
        Stopwatch timer = new Stopwatch();
        timer.Start();
        Console.WriteLine($"Opening file...");

        int numOfCpus = 12;
        var baseFile = File.OpenText(args[0]);
        var totalBytes = baseFile.BaseStream.Length;
        var bytesPerCpu = totalBytes / numOfCpus - 5000; //

        // split the bytes in <processors> blocks
        baseFile.BaseStream.Position = bytesPerCpu;
        long start = 0;
        for (int i = 0; i < numOfCpus - 1; i++)
        {
            do
            {
                var c = baseFile.Read();
                if (c != '\n')
                {
                    baseFile.BaseStream.Position++;
                }
                else
                {
                    mapOfBytePositions.Add(i, (start, baseFile.BaseStream.Position));
                    baseFile.BaseStream.Position++; // next start
                    start = baseFile.BaseStream.Position;
                    baseFile.BaseStream.Position = baseFile.BaseStream.Position + bytesPerCpu;
                    break;
                }
            } while (true);
        }
        // the last one get's the rest
        mapOfBytePositions.Add(numOfCpus - 1, (start, totalBytes));
        List<List<string>> listOfallBlocks = new List<List<string>>();

        baseFile.Close(); // wach loop will use a different file.

        int cpuLoops = 0;
        while (cpuLoops < numOfCpus)
        {
            List<string> list = new List<string>();

            var startPoint = mapOfBytePositions[cpuLoops].Item1;
            var stopByte = mapOfBytePositions[cpuLoops].Item2;
            var data = File.OpenText(args[0]);
            string line = string.Empty;

            for (long i = startPoint; i < stopByte; i++)
            {
                int leChar = data.Read();
                if (leChar != -1 && leChar != '\n')
                {
                    line += (char)leChar;
                }
                else if (line != string.Empty) // might be just a single break line
                {
                    if(line.IndexOf(";")  == -1)
                    {
                        throw new Exception("WTF");

                    }
                    list.Add(line);
                    line = string.Empty;
                    lines++;
                   // linesTemp++;
                }
                else
                {
                    throw new Exception("WTF");
                }
            }
            listOfallBlocks.Add(list); 

            // advance processor 
            /*if (linesTemp >= numberLinesPerCpu)
            {
                //send to thread
                _ = ProcessList(cpuLoops, list);
                cpuLoops++;
            }*/

            cpuLoops++;
        }



            // 
            Console.WriteLine($"Processing...");
       

        long buffSize = totalBytes / numOfCpus;
        int numberLinesPerCpu = lineToProcess / numOfCpus;
        int linesTemp = 0;
       // int cpuLoops = 0;

        while (cpuLoops < numOfCpus)
        {
            allResults.Add(new Dictionary<string, Station>());
            cpuLoops++;
        }
        cpuLoops = 0;

        while (cpuLoops < numOfCpus) 
        {
            List<string> list = new List<string>();
            var buff = new char[buffSize+5000];                       
            baseFile.ReadBlock(buff, 0, (int)buffSize);

            // Try to find an EOL
            do
            {
                var foo = baseFile.Peek();
                if (foo != '\n' && foo != -1)
                {
                    buffSize++;
                    buff[buffSize] = (char)baseFile.Read();
                }
                else
                {
                    break;
                }
            }
            while (true);

            string line = string.Empty;
            for (int i = 0; i < buffSize; i++)
            {
                if (buff[i] != '\n' )
                {
                    line += buff[i];
                }else if(line != string.Empty) // might be just a single break line
                {
                    list.Add(line);
                    line = string.Empty;
                    lines++;
                    linesTemp++;
                }
            }
            //listOfallBlocks.Add(list); 

            // advance processor 
            if (linesTemp >= numberLinesPerCpu)
            {
                //send to thread
                _ = ProcessList(cpuLoops, list);
                cpuLoops++;
            }
        }        

        //  TODO -> Get the content together
        stations = IntegrateResults(allResults);

        // Creating output file
        foreach (var station in stations)
        {
            Console.WriteLine($"{station.Key}. min: {station.Value.min} max: {station.Value.max} avg: {station.Value.avg.ToString("0.##")}");
        }
        timer.Stop();

        Console.WriteLine(" ");
        Console.WriteLine($"File with {lines} registries was processed in {timer.Elapsed.TotalMilliseconds/1000}s ");
    }

    private static async Task<bool> ProcessList(int cpu, List<string> list)
    {
        Dictionary<string, Station> stations = new Dictionary<string, Station>();

        foreach (var lined in list)
        {
            var point = lined.Split(';');
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
        }

        allResults[cpu] = stations;

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

                    stations[listOfResults.Key].avg = (stations[listOfResults.Key].avg + listOfResults.Value.avg) / 2;
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