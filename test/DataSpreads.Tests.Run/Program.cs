/*
 * Copyright (c) 2017-2019 Victor Baybekov (DataSpreads@DataSpreads.io, @DataSpreads)
 *
 * This file is part of DataSpreads.
 *
 * DataSpreads is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * DataSpreads is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with DataSpreads.  If not, see <http://www.gnu.org/licenses/>.
 *
 * DataSpreads works well as an embedded realtime database,
 * but it works much better when connected to a network!
 *
 * Please sign up for free at <https://dataspreads.io/HiGitHub>,
 * download our desktop app with UI and background service,
 * get an API token for programmatic access to our network,
 * and enjoy how fast and securely your data spreads to the World!
 */

using DataSpreads.Tests.StreamLogs;
using System;
using System.Diagnostics;
using System.Threading.Tasks;

namespace DataSpreads.Tests.Run
{
    internal class ConsoleListener : TraceListener
    {
        public override void Write(string message)
        {
            Console.Write(message);
        }

        public override void WriteLine(string message)
        {
            Console.WriteLine(message);
        }
    }

    internal class Program
    {
        private static async Task Main(string[] args)
        {
            Console.WriteLine("Max memory: " + Process.GetCurrentProcess().MaxWorkingSet);

            Trace.Listeners.Add(new ConsoleListener());

            //DataSpreads.StateManagement.StartupConfig.StreamLogBufferPoolFlags = LMDBEnvironmentFlags.NoSync; //  | LMDBEnvironmentFlags.NoMetaSync;
            //DataSpreads.StateManagement.StartupConfig.StreamLogChunksIndexFlags = LMDBEnvironmentFlags.NoSync; //  | LMDBEnvironmentFlags.NoMetaSync; // ; // LMDBEnvironmentFlags.NoSync |

            //var memPressure = new List<byte[]>();

            //for (int i = 0; i < 0; i++)
            //{
            //    var buffer = new byte[1024 * 1024 * 1024 / 10];
            //    ((Span<byte>)buffer).Clear();
            //    memPressure.Add(buffer);
            //}

            var test = new StreamLogTests();
            test.ClaimPerformance();

            //test.CouldInitStreamLog();
            //test.CouldCommitToStreamLog();
            //test.CouldRotateStreamLog();
            //test.CouldWriteAndReadLog();
            //test.CouldGetByVersionFromUnpacked();
            //await test.CouldWriteAndReadLogWithWalAckAsync();
            //await test.CouldWriteAndReadLogWithWalAckParalell();
            //test.CouldWriteLargeValues();

            
            Console.WriteLine("OK. Finished before forced GC.");

            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, true);
            GC.WaitForPendingFinalizers();
            
            Console.WriteLine("OK. Exiting.");
        }

        //private static async Task StartConcurrentTest(string[] args)
        //{
        //    if (args == null || args.Length == 0)
        //    {
        //        var test = new RepoOldTests.RepoOldTests();
        //        test.Clear();

        //        var filename = typeof(Program).Assembly.Location;

        //        // var filename = Process.GetCurrentProcess().MainModule.FileName;
        //        var process1 = Process.Start(new ProcessStartInfo
        //        {
        //            Arguments = filename + " 1",
        //            UseShellExecute = false,
        //            RedirectStandardOutput = true,
        //            FileName = "dotnet",
        //            CreateNoWindow = false,
        //            WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory,
        //        });

        //        process1.ProcessorAffinity = (IntPtr)0b0011; // use only any of the first 4 available processors
        //        process1.OutputDataReceived += (s, e) => Console.WriteLine("1: " + DateTime.UtcNow.ToString("O") + " | " + e.Data);
        //        process1.BeginOutputReadLine();

        //        var process2 = Process.Start(new ProcessStartInfo
        //        {
        //            Arguments = filename + " 2",
        //            UseShellExecute = false,
        //            RedirectStandardOutput = true,
        //            FileName = "dotnet",
        //            CreateNoWindow = false,
        //            WorkingDirectory = AppDomain.CurrentDomain.BaseDirectory
        //        });
        //        process2.ProcessorAffinity = (IntPtr)0b1100; // use only any of the first 4 available processors
        //        process2.OutputDataReceived += (s, e) => Console.WriteLine("2: " + DateTime.UtcNow.ToString("O") + " | " + e.Data);
        //        process2.BeginOutputReadLine();

        //        process2.WaitForExit();

        //        process1.WaitForExit();
        //    }
        //    else
        //    {
        //        //Debugger.Launch();
        //        //Debugger.Break();
        //        var id = args[0];
        //        // var test = new StreamLogs.StreamLogTests();
        //        throw new Exception();
        //        // await test.CouldListenToZeroLogWithConcurrentWrites(id);

        //        //var test = new StreamLogs.RepoTests();
        //        //await test.CouldReadDataStreamWhileWritingFromTwoWriters();

        //        Console.WriteLine("Finished test");
        //        Environment.Exit(0);
        //    }
        //}
    }
}
