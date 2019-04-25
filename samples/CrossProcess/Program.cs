using DataSpreads;
using DataSpreads.Tests.Run;
using Spreads;
using Spreads.DataTypes;
using Spreads.Serialization;
using Spreads.Utils;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace CrossProcess
{
    [StructLayout(LayoutKind.Sequential)]
    [BinarySerialization(8)]
    public struct Message
    {
        public long Field1;
        //public long Field2;
        //public long Field3;
        //public long Field4;
        //public long Field5;
        //public long Field6;
        //public long Field7;
        //public long Field8;

        public Message(long value)
        {
            this = default;
            Field1 = value;
        }
    }

    internal class Program
    {
        private static DataStore DS;
        private static Repo Repo;

        // Currently very contented load is unstable with random errors on different threads.
        // This is the main goal to make such load stable - then the whole lib is stable.
        // One nasty thing is that RetainableMemory buffers still leak and are finalized randomly,
        // we throw when finalizing retained buffers which kills the app.
        private static int StreamCount = 12; // Environment.ProcessorCount * 4;

        private const int ItemsPerStreamCount = 100_000_000;

        public static async Task Main(string[] args)
        {
            // Print diagnostic info to console.
            Trace.Listeners.Add(new ConsoleListener());

            // Traces ctor stack of leaked buffers. Slow and should normally be off.
            Settings.DoDetectBufferLeaks = false;

            // Bound checks and other correctness checks. Fast and should be ON until the lib is stable.
            Settings.DoAdditionalCorrectnessChecks = false;

            var path = Path.GetFullPath("G:/sample_data_stores/cross_process");

            if (args.Length == 0)
            {
                if (Directory.Exists(path))
                {
                    Directory.Delete(path, true);
                }
            }

            DS = DataStore.Create("CrossProcess", path);
            Repo = await DS.Local.CreateRepoAsync("cross_process", new Metadata("CrossProcess sample repo"));

            for (int i = 0; i < StreamCount; i++)
            {
                // CreateStreamAsync returns true is a stream was created or false if a stream already existed
                var _ = await Repo.CreateStreamAsync<Message>($"req_{i}", new Metadata($"Request stream {i}"),
                    DataStreamCreateFlags.Binary).ConfigureAwait(false);
                await Repo.CreateStreamAsync<Message>($"res_{i}", new Metadata($"Response stream {i}"),
                    DataStreamCreateFlags.Binary).ConfigureAwait(false);
            }

            using (Benchmark.Run("CrossProcess", 2 * ItemsPerStreamCount * StreamCount))
            {
                var sw = new Stopwatch();
                sw.Start();
                if (args.Length == 1)
                {
                    if (args[0] == "a")
                    {
                        Console.WriteLine("Starting Requester");
                        await Requester().ConfigureAwait(false);
                    }
                    else if (args[0] == "b")
                    {
                        Console.WriteLine("Starting Responder");
                        await Responder().ConfigureAwait(false);
                    }
                }
                else
                {
                    // TODO Currently races in Packer when run from separate processes.
#if XXX
                    var req = Requester();
                    var res = Responder();
                    await req.ConfigureAwait(false);
                    await res.ConfigureAwait(false);
#else
                    var processA = Process.Start(new ProcessStartInfo
                    {
                        Arguments = "a",
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        FileName = "CrossProcess",
                        CreateNoWindow = false,
                        WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory,
                    });
                    // processA.ProcessorAffinity = (IntPtr)0b0000_1111;
                    processA.OutputDataReceived += (s, e) => Console.WriteLine("A| " + e.Data);
                    processA.BeginOutputReadLine();

                    var processB = Process.Start(new ProcessStartInfo
                    {
                        Arguments = "b",
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        FileName = "CrossProcess",
                        CreateNoWindow = false,
                        WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory,
                    });
                    // processB.ProcessorAffinity = (IntPtr)0b1111_0000;
                    processB.OutputDataReceived += (s, e) => Console.WriteLine("B| " + e.Data);
                    processB.BeginOutputReadLine();

                    processB.WaitForExit();
                    processA.WaitForExit();
#endif
                }
                sw.Stop();
                Console.WriteLine($"Finished {ItemsPerStreamCount:N} items in {StreamCount} streams in {sw.ElapsedMilliseconds:N0} msecs ({(2 * StreamCount * ItemsPerStreamCount * 0.001 / sw.ElapsedMilliseconds):N2}M messages/sec).");

                DS.Dispose();
            }
        }

        public static async Task Requester()
        {
            try
            {
                var tasks = new List<Task>();
                for (int i = 0; i < StreamCount; i++)
                {
                    tasks.Add(Requester(i));
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Environment.FailFast("Exception in Requester", ex);
            }
        }

        public static async Task Requester(int id)
        {
            using (var req = (await Repo.OpenStreamWriterAsync<Message>($"req_{id}", WriteMode.LocalSync,
                bytesPerMinuteHint: 8000_000).ConfigureAwait(false)).WithTimestamp(KeySorting.NotEnforced))
            using (var res = await Repo.OpenStreamAsync<Message>($"res_{id}").ConfigureAwait(false))
            {
                var reqT = Task.Run(async () =>
                {
                    try
                    {
                        for (int i = 0; i < ItemsPerStreamCount; i++)
                        {
                            var version = await req.TryAppend(new Message(i), Timestamp.Now).ConfigureAwait(false);
                            if (version == 0)
                            {
                                throw new Exception("Cannot append to req stream");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Environment.FailFast("Exception in reqT", ex);
                    }
                });

                var resT = Task.Run(async () =>
                {
                    try
                    {
                        var i = 0;
                        await foreach (var keyValuePair in res) // Note: Async stream
                        {
                            i++;
                            if (i > 0 && (i * 5) % ItemsPerStreamCount == 0)
                            {
                                Console.WriteLine($"Processed {id}: {i:N0}");
                            }

                            if (i == ItemsPerStreamCount)
                            {
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Environment.FailFast("Exception in resT", ex);
                    }
                });

                await reqT.ConfigureAwait(false);
                await resT.ConfigureAwait(false);
            }
        }

        public static async Task Responder()
        {
            try
            {
                var tasks = new List<Task>();
                for (int i = 0; i < StreamCount; i++)
                {
                    tasks.Add(Responder(i));
                }

                await Task.WhenAll(tasks);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in Responder: " + ex);
                throw;
            }
        }

        public static async Task Responder(int id)
        {
            using (var req = await Repo.OpenStreamAsync<Message>($"req_{id}"))
            using (var res = await Repo.OpenStreamWriterAsync<Message>($"res_{id}", WriteMode.LocalSync,
                bytesPerMinuteHint: 8000_000))
            {
                var t = Task.Run(async () =>
                {
                    try
                    {
                        var i = 0;
                        await foreach (var keyValuePair in req) // Note: Async stream
                        {
                            await res.TryAppend(new Message(keyValuePair.Value.Value.Field1 * 2)).ConfigureAwait(false);
                            i++;
                            if (i == ItemsPerStreamCount)
                            {
                                break;
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Environment.FailFast("Exception in responder", ex);
                    }
                });

                await t;
            }
        }
    }
}
