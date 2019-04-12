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

using DataSpreads.Buffers;
using NUnit.Framework;
using Spreads.Utils;
using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

#pragma warning disable 618

namespace DataSpreads.Tests.Buffers
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public unsafe class DirectFileTests
    {
        [Test]
        public void CouldOpenSparse()
        {
            var path = TestUtils.GetPath();
            var df = new DirectFile(Path.Combine(path, "test.file"), 64L * 1024 * 1024 * 1024, true,
                FileOptions.WriteThrough, sparse: true);

            Unsafe.InitBlockUnaligned(df.DirectBuffer.Data, 0, 1024);

            df.Dispose();
        }

        [Test]
        public void CouldReadUnallocatedSparse()
        {
            var path = TestUtils.GetPath();
            var df = new DirectFile(Path.Combine(path, "test.file"), 1L * 1024 * 1024, true,
                FileOptions.WriteThrough, sparse: true);

            var sum = 0;
            for (long i = 0; i < df.Length; i++)
            {
                sum += unchecked(df.Buffer[i]);
                // Console.WriteLine(df.Buffer[i]);
            }

            Assert.AreEqual(0, sum);

            df.Dispose();
        }

        [Test]
        public void CouldWriteToWriteThroughFile()
        {
            var path = TestUtils.GetPath();
            var df = new DirectFile(Path.Combine(path, "test.file"), 1024 * 1024, true,
                FileOptions.WriteThrough);

            df.DirectBuffer.Write(0, 123L);
            var bytes = new byte[] { 123 };

            df._va.WriteArray(0, bytes, 0, 1);
            var ums = df._mmf.CreateViewStream(0, 1024 * 1024);
            ums.Write(bytes, 0, 1);
            ums.Dispose();
            df.Flush(true);
            df.Dispose();
        }

        [Test]
        public void FileConcurrentWriteRead()
        {
            var pagesPerWrite = new[] { 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512, 1024 };
            var pageSize = 512;

            var count = TestUtils.GetBenchCount(5_000L, 50);

            var path = TestUtils.GetPath(clear: true);
            var file = Path.Combine(path, "test.file");
            using (var fs = new FileStream(file, FileMode.CreateNew))
            {
                fs.SetLength(count * pageSize);
            }

            foreach (var mult in pagesPerWrite)
            {
                var sem = new SemaphoreSlim(0, (int)count);
                var page = new byte[pageSize * mult];

                var rt = Task.Run(() =>
                {
                    using (var df = new DirectFile(file, 0L, false, FileOptions.SequentialScan))
                    //using (var fs2 = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite,
                    //    pageSize * mult,
                    //    FileOptions.None))
                    //using (var memoryMappedFile =
                    //    MemoryMappedFile.CreateFromFile(fs2, Guid.NewGuid().ToString(), count * pageSize,
                    //        MemoryMappedFileAccess.ReadWrite,
                    //        HandleInheritability.None, false))
                    //using (var memoryMappedViewAccessor = memoryMappedFile.CreateViewAccessor())
                    {
                        using (Benchmark.Run("Read " + mult, count * pageSize))
                        {
                            var i = 0;
                            var page2 = new byte[pageSize * mult];

                            while (i < count / mult)
                            {
                                sem.Wait();
                                var sp = df.DirectBuffer.Span.Slice(pageSize * i * mult, pageSize * mult);
                                //memoryMappedViewAccessor.ReadArray(pageSize * i * (long)mult, page2, 0,
                                //    pageSize * mult);
                                if (!((ReadOnlySpan<byte>)sp).SequenceEqual(page))
                                {
                                    Assert.Fail("Pages are not equal");
                                }
                                i++;
                            }
                        }
                    }
                });

                using (Benchmark.Run("Write " + mult, count * pageSize))
                {
                    using (var fs = new FileStream(file, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite,
                        pageSize * mult, FileOptions.WriteThrough)) //| WAL.WALWriter.FILE_FLAG_NO_BUFFERING)) //
                    {
                        fs.SetLength(count * pageSize);
                        new Random(123).NextBytes(page);
                        for (int i = 0; i < count / mult; i++)
                        {
                            fs.Position = pageSize * i * (long)mult;
                            fs.Write(page, 0, page.Length);

                            sem.Release();
                        }
                    }
                }

                rt.Wait();
            }

            Benchmark.Dump();
        }
    }
}
