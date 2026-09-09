using System.Globalization;

namespace MessageDispatching.ScalingBenchmarks;

internal static class BenchmarkDefaults
{
    public const double DurationSeconds = 3.0;
    public const int ReportIntervalMilliseconds = 250;
    public const int ScalingSampleMilliseconds = 250;
    public const int MaxParallelism = 32;
    public const int KeyBatchSize = 8;
    public const int ProducerBatchSize = 256;
    public const int ProducerPauseMilliseconds = 2;
}

internal sealed class BenchmarkOptions
{
    public required IReadOnlyList<string> Scenarios { get; init; }

    public TimeSpan Duration { get; init; } = TimeSpan.FromSeconds(BenchmarkDefaults.DurationSeconds);

    public TimeSpan ReportInterval { get; init; } =
        TimeSpan.FromMilliseconds(BenchmarkDefaults.ReportIntervalMilliseconds);

    public TimeSpan ScalingSampleInterval { get; init; } =
        TimeSpan.FromMilliseconds(BenchmarkDefaults.ScalingSampleMilliseconds);

    public int MaxParallelism { get; init; } = BenchmarkDefaults.MaxParallelism;

    public bool ShowHelp { get; init; }

    public bool ListScenarios { get; init; }

    public static BenchmarkOptions Parse(string[] args)
    {
        var requestedScenarios = new List<string>();
        var durationSeconds = BenchmarkDefaults.DurationSeconds;
        var reportMilliseconds = BenchmarkDefaults.ReportIntervalMilliseconds;
        var scalingSampleMilliseconds = BenchmarkDefaults.ScalingSampleMilliseconds;
        var maxParallelism = BenchmarkDefaults.MaxParallelism;
        var showHelp = false;
        var listScenarios = false;

        for (var index = 0; index < args.Length; index++)
        {
            var argument = args[index];
            switch (argument)
            {
                case "-h":
                case "--help":
                    showHelp = true;
                    break;

                case "--list":
                    listScenarios = true;
                    break;

                case "-s":
                case "--scenario":
                    AddScenarioValues(requestedScenarios, ReadValue(args, ref index, argument));
                    break;

                case "--duration-seconds":
                    durationSeconds = ParseDouble(ReadValue(args, ref index, argument), argument);
                    break;

                case "--report-ms":
                    reportMilliseconds = ParseInt(ReadValue(args, ref index, argument), argument);
                    break;

                case "--scaling-sample-ms":
                    scalingSampleMilliseconds = ParseInt(ReadValue(args, ref index, argument), argument);
                    break;

                case "--max-parallelism":
                    maxParallelism = ParseInt(ReadValue(args, ref index, argument), argument);
                    break;

                default:
                    if (argument.StartsWith("-", StringComparison.Ordinal))
                    {
                        throw new ArgumentException($"Unknown option '{argument}'.");
                    }

                    AddScenarioValues(requestedScenarios, argument);
                    break;
            }
        }

        if (!double.IsFinite(durationSeconds) || durationSeconds < 0.5 || durationSeconds > 300)
        {
            throw new ArgumentException("--duration-seconds must be between 0.5 and 300.");
        }

        if (reportMilliseconds < 50 || reportMilliseconds > 60_000)
        {
            throw new ArgumentException("--report-ms must be between 50 and 60000.");
        }

        if (scalingSampleMilliseconds < 50 || scalingSampleMilliseconds > 60_000)
        {
            throw new ArgumentException("--scaling-sample-ms must be between 50 and 60000.");
        }

        if (maxParallelism < 2 || maxParallelism > 1024)
        {
            throw new ArgumentException("--max-parallelism must be between 2 and 1024.");
        }

        var scenarios = ScenarioCatalog.Resolve(requestedScenarios);
        return new BenchmarkOptions
        {
            Scenarios = scenarios,
            Duration = TimeSpan.FromSeconds(durationSeconds),
            ReportInterval = TimeSpan.FromMilliseconds(reportMilliseconds),
            ScalingSampleInterval = TimeSpan.FromMilliseconds(scalingSampleMilliseconds),
            MaxParallelism = maxParallelism,
            ShowHelp = showHelp,
            ListScenarios = listScenarios
        };
    }

    public static void WriteUsage(TextWriter writer)
    {
        writer.WriteLine("Lightweight dynamic-scaling benchmarks for MessageDispatching (.NET 8).");
        writer.WriteLine();
        writer.WriteLine("Usage:");
        writer.WriteLine("  dotnet run --project benchmarks/MessageDispatching.ScalingBenchmarks -- [scenario ...] [options]");
        writer.WriteLine("  dotnet run --project benchmarks/MessageDispatching.ScalingBenchmarks -- --scenario keyed-homogeneous");
        writer.WriteLine();
        writer.WriteLine("With no scenario, all scenarios run. Output is CSV: periodic sample rows followed by");
        writer.WriteLine("one summary row per scenario. Diagnostics and validation failures go to stderr.");
        writer.WriteLine();
        writer.WriteLine("Options:");
        writer.WriteLine("  -s, --scenario <name[,name]>  Select one or more scenarios; may be repeated.");
        writer.WriteLine("      --list                    List scenario names.");
        writer.WriteLine($"      --duration-seconds <n>    Sustained input duration (default {BenchmarkDefaults.DurationSeconds:F1}).");
        writer.WriteLine($"      --report-ms <n>           CSV sample period (default {BenchmarkDefaults.ReportIntervalMilliseconds}).");
        writer.WriteLine($"      --scaling-sample-ms <n>   Dynamic scaling sample period (default {BenchmarkDefaults.ScalingSampleMilliseconds}).");
        writer.WriteLine($"      --max-parallelism <n>     Worker bound (default {BenchmarkDefaults.MaxParallelism}).");
        writer.WriteLine("                                  keyed-four-hot-keys always uses 32.");
        writer.WriteLine("  -h, --help                    Show this help.");
    }

    private static void AddScenarioValues(List<string> scenarios, string value)
    {
        foreach (var item in value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            scenarios.Add(item);
        }
    }

    private static string ReadValue(string[] args, ref int index, string option)
    {
        if (++index >= args.Length)
        {
            throw new ArgumentException($"Option '{option}' requires a value.");
        }

        return args[index];
    }

    private static int ParseInt(string value, string option)
    {
        if (!int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new ArgumentException($"Option '{option}' requires an integer value.");
        }

        return parsed;
    }

    private static double ParseDouble(string value, string option)
    {
        if (!double.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed))
        {
            throw new ArgumentException($"Option '{option}' requires a numeric value.");
        }

        return parsed;
    }
}
