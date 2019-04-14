using DataSpreads;
using Spreads;
using Spreads.DataTypes;
using Spreads.Serialization;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace Ingest
{
    [StructLayout(LayoutKind.Sequential)]
    [BinarySerialization(24)]
    public struct Payload
    {
        [ThreadStatic]
        public static Random Rng;

        public SmallDecimal Field1;
        public SmallDecimal Field2;
        public int Field3;
        public int Field4;

        public Payload(SmallDecimal field1, SmallDecimal field2, int field3, int field4)
        {
            Field1 = field1;
            Field2 = field2;
            Field3 = field3;
            Field4 = field4;
        }

        public static Payload Create()
        {
            var rng = Rng ?? (Rng = new Random());
            var valF = rng.NextDouble();
            var valI = rng.Next(0, 100000);

            return new Payload(new SmallDecimal(valF, 3), new SmallDecimal(valF * 100, 3), valI, valI * 100);
        }
    }

    internal class Program
    {
        private static DataStore DS;
        private static Repo Repo;

        private static int StreamCount = 100;

        private const int ItemCount = 100_000_000;
        private static long TotalCount = 0;

        private static async Task Main(string[] args)
        {
            Settings.DoDetectBufferLeaks = false;

            // Bound checks and other correcntess checks. Fast and should be ON until the lib is stable.
            Settings.DoAdditionalCorrectnessChecks = false;

            var path = Path.GetFullPath("G:/sample_data_stores/ingest");

            if (args.Length == 0)
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, true);
                }
            }

            DS = DataStore.Create("ingest", path);
            Repo = await DS.Local.CreateRepoAsync("ingest", new Metadata("ingest sample repo"));

            for (int i = 0; i < StreamCount; i++)
            {
                // CreateStreamAsync returns true is a stream was created or false if a stream already existed
                var _ = await Repo.CreateStreamAsync<Payload>($"payload_{i}", new Metadata($"Payload stream {i}"),
                    DataStreamCreateFlags.Binary).ConfigureAwait(false);
            }

            var streams = new List<DataStreamWriterWithTimestamp<Payload>>();

            for (int i = 0; i < StreamCount; i++)
            {
                var streamWriter = (await Repo.OpenStreamWriterAsync<Payload>($"payload_{i}", WriteMode.BatchWrite)
                    .ConfigureAwait(false)).WithTimestamp(KeySorting.NotEnforced);

                streams.Add(streamWriter);
            }

            var tasks = new List<Task>();

            for (int i = 0; i < StreamCount; i++)
            {
                var sw = streams[i];

                tasks.Add(Task.Run(async () =>
                {
                    for (int j = 0; j < ItemCount; j++)
                    {
                        await sw.TryAppend(Payload.Create(), Timestamp.Now);
                        Interlocked.Increment(ref TotalCount);
                    }
                }));
            }

            var cts = new CancellationTokenSource();

            var rr = new Thread(() =>
            {
                var previousCount = TotalCount;
                var sw = new Stopwatch();
                while (!cts.IsCancellationRequested)
                {
                    sw.Restart();
                    Thread.Sleep(1000);
                    var tc = TotalCount;
                    var c = tc - previousCount;
                    previousCount = tc;
                    sw.Stop();
                    var log =
                        $"Written items total {tc:N0}, raw payload MB {(tc * 24.0 / (1024 * 1024)):N0}, in last {sw.ElapsedMilliseconds} msecs: {c:N0}, Mops {(c * 0.001/ sw.ElapsedMilliseconds):N2} \n";
                    File.AppendAllText(Path.Combine(path, "log.txt"), log);
                    Console.Write(log);
                }
            });
            rr.Priority = ThreadPriority.Highest;

            rr.Start();

            await Task.WhenAll(tasks);
            cts.Cancel();
            rr.Join();

            Console.WriteLine("Finished");
        }
    }
}
