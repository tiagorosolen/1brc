using System.Diagnostics;
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

    private static void Main(string[] args)
    {
        if(args.Length == 0) 
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
        //var data = File.ReadAllLines(args[0]);
        var data = File.OpenText(args[0]);

        // 
        Console.WriteLine($"Processing...");

        int r = 0;
        while (data.Peek() != -1)
        {
            var point = data.ReadLine().Split(';');
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
            r++;
        }

        // Creating output file
        foreach(var station in stations)
        {
            Console.WriteLine($"{station.Key}. min: {station.Value.min} max: {station.Value.max} avg: {station.Value.avg.ToString("0.##")}");
        }
        timer.Stop();

        Console.WriteLine(" ");
        Console.WriteLine($"File with {r} registries was processed in {timer.Elapsed.TotalMilliseconds/1000}s ");
    }
}