using DataSpreads;
using DataSpreads.Tests.Run;
using Spreads;
using Spreads.DataTypes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace AsTimeSeries
{
    internal class Program
    {
        private static DataStore DS;
        private static Repo Repo;

        private const int ItemCount = 1_000;
        private static long TotalCount = 0;

        private static async Task Main(string[] args)
        {
            Trace.Listeners.Add(new ConsoleListener());
            Settings.ZstdCompressionLevel = 1;
            Settings.DoDetectBufferLeaks = true;

            // Bound checks and other correctness checks. Fast and should be ON until the lib is stable.
            Settings.DoAdditionalCorrectnessChecks = true;

            var path = Path.GetFullPath("G:/sample_data_stores/as_time_series");

            if (args.Length == 0)
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, true);
                }
            }

            DS = DataStore.Create("ingest", path);
            Repo = await DS.Local.CreateRepoAsync("ingest", new Metadata("ingest sample repo"));

            // CreateStreamAsync returns true is a stream was created or false if a stream already existed
            var _ = await Repo.CreateStreamAsync<double>($"payload_even", new Metadata($"Payload stream A"),
                DataStreamCreateFlags.Binary).ConfigureAwait(false);
            await Repo.CreateStreamAsync<double>($"payload_odd", new Metadata($"Payload stream B"),
                DataStreamCreateFlags.Binary).ConfigureAwait(false);

            var streamWriterA = (await Repo.OpenStreamWriterAsync<double>($"payload_even",
                    WriteMode.BatchWrite)
                .ConfigureAwait(false)).WithTimestamp(KeySorting.Strong);
            var streamWriterB = (await Repo.OpenStreamWriterAsync<double>($"payload_odd",
                    WriteMode.BatchWrite)
                .ConfigureAwait(false)).WithTimestamp(KeySorting.Strong);

            var producer = Task.Run(async () =>
            {

                for (int i = 0; i < ItemCount; i++)
                {
                    if (i % 2 == 0)
                    {
                        await streamWriterA.TryAppend(i, new Timestamp(i));
                    }
                    else
                    {
                        await streamWriterB.TryAppend(i, new Timestamp(i));
                    }
                }
                
            });


            var consumer = Task.Run(async () =>
            {
                var tsEven = (await Repo.OpenStreamAsync<double>($"payload_even")).AsTimeSeries();
                var tsOdd = (await Repo.OpenStreamAsync<double>($"payload_odd")).AsTimeSeries();


                var realTimeJoin = tsEven.Repeat().Zip(tsOdd.Repeat());
                var c = 0;
                await foreach (var pair in realTimeJoin) // Note: Async stream
                {
                    Console.WriteLine($"TS: {pair.Key} - Values: {pair.Value.Item1} - {pair.Value.Item2}");
                    c++;
                    if (c == ItemCount)
                    {
                        break;
                    }
                }

            });

            await producer;
            await consumer;

            Console.WriteLine("Finished");
        }
    }
}
