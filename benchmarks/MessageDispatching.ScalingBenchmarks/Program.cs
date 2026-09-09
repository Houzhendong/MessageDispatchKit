using MessageDispatching.ScalingBenchmarks;

BenchmarkOptions options;

try
{
    options = BenchmarkOptions.Parse(args);
}
catch (ArgumentException exception)
{
    Console.Error.WriteLine(exception.Message);
    Console.Error.WriteLine();
    BenchmarkOptions.WriteUsage(Console.Error);
    return 2;
}

if (options.ShowHelp)
{
    BenchmarkOptions.WriteUsage(Console.Out);
    return 0;
}

if (options.ListScenarios)
{
    ScenarioCatalog.WriteList(Console.Out);
    return 0;
}

CsvReporter.WriteHeader();

var failed = false;
foreach (var scenarioName in options.Scenarios)
{
    try
    {
        var result = await ScenarioRunner.RunAsync(scenarioName, options);
        CsvReporter.WriteSummary(result);

        if (!result.Success)
        {
            failed = true;
            Console.Error.WriteLine($"{scenarioName}: {result.Status}");
        }
    }
    catch (Exception exception)
    {
        failed = true;
        CsvReporter.WriteFailure(scenarioName, exception);
        Console.Error.WriteLine($"{scenarioName}: {exception.GetType().Name}: {exception.Message}");
    }
}

return failed ? 1 : 0;
