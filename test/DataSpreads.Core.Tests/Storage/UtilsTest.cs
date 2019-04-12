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

using System.IO;
using DataSpreads.Buffers;
using NUnit.Framework;
using Spreads.Utils;

namespace DataSpreads.Tests.Storage
{
    [TestFixture]
    public class UtilsTests
    {
        
#if NETCOREAPP2_1
        [Test, Explicit("long running")]
        public void OpenCloseFiles()
        {
            var path = TestUtils.GetPath();
            var count = 1_000;
            var buffer = new byte[4096 * 1];
            using (Benchmark.Run("Open/Close file", count * 1_000))
            {
                for (int i = 0; i < count; i++)
                {
                    using (var fs = new FileStream(Path.Combine(path, "test_file.bin"),
                        FileMode.OpenOrCreate,
                        FileAccess.ReadWrite,
                        FileShare.ReadWrite,
                        1,
                        FileOptions.SequentialScan |
                        FileOptions.WriteThrough |
                        (FileOptions)0x20000000))
                    {
                        if (i == 0)
                        {
                            fs.SetLength(buffer.Length);
                        }

                        buffer[0] = (byte)(i % 255);
                        buffer[4095] = (byte)(i % 255);
                        fs.Write(buffer);
                        // fs.SetLength(i);
                        // fs.Flush();
                    }
                }
            }
            Benchmark.Dump();
        }
#endif

        [Test, Explicit("long running")]
        public void OpenCloseDirectFiles()
        {
            var path = TestUtils.GetPath("UtilsTests");
            var count = 10_000;
            using (Benchmark.Run("Open/Close view", count * 1_000))
            {
                using (var directFile = new DirectFile(Path.Combine(path, "test_file.bin"), 4096, true))
                {
                    for (int i = 1; i <= count; i++)
                    {
                        directFile.Grow(4096 + 4096 * i);
                        directFile.Buffer.Write<long>(4096 * i - 8, i);
                    }
                }
            }
            Benchmark.Dump();

            using (var directFile = new DirectFile(Path.Combine(path, "test_file.bin"), 4096, true))
            {
                for (int i = 1; i <= count; i++)
                {
                    directFile.Grow(4096 + 4096 * i);
                    var val = directFile.Buffer.Read<long>(4096 * i - 8);
                    Assert.AreEqual(i, val);
                }
            }
        }
    }
}
