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

using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;
using Spreads.Utils;

namespace DataSpreads.Tests
{
    [TestFixture]
    public class RepoTests
    {
        [Test]
        public void CouldGetDefaultReposAsStaticProperties()
        {
            var my = Repo.My;
            Assert.AreEqual("~/", my.DataPath);

            var local = Repo.Local;
            Assert.AreEqual("_/", local.DataPath);
        }

        [Test]
        public async Task CouldCreateNewSubRepoAsync()
        {
            var sub = await Repo.My.CreateRepoAsync("sub", new Metadata("Sub repo"));
            Assert.AreEqual("~/sub/", sub.DataPath);

            sub = await Repo.Local.CreateRepoAsync("sub", new Metadata("Sub repo"));
            Assert.AreEqual("_/sub/", sub.DataPath);
            Assert.AreEqual(sub.Metadata.Description, "Sub repo");
            Assert.IsTrue(sub.MetadataRecord.RepoId < 0);

            // var stream = sub.NewStream<>().NewStream()
        }

        [Test]
        public async Task CouldCreateNewStreamAsync()
        {
            var created = await Repo.Local.CreateStreamAsync<long>("test_stream", new Metadata("test stream"));
            Console.WriteLine("Created: " + created);
        }

        [Test]
        public async Task CouldCreateNewStreamAndOpenWriterAsync()
        {
            var description = "test stream description";
            var created = await Repo.Local.CreateStreamAsync<long>("test_stream", new Metadata(description).WithTimezone("invalid tz"), DataStreamCreateFlags.Binary);
            Console.WriteLine("Created: " + created);

            var reader = await Repo.Local.OpenStreamAsync<long>("test_stream");

            var writer = await Repo.Local.OpenStreamWriterAsync<long>("test_stream", bytesPerMinuteHint:1000_000);

            var count = TestUtils.GetBenchCount(50_000_000, 1000);

            var start = reader.Count();

            using (Benchmark.Run("Write", count))
            {
                
                for (int i = start; i < start + count; i++)
                {
                    var version = await writer.TryAppend(i);
                    if (version == 0)
                    {
                        Assert.Fail();
                    }
                }
                
            }


            Assert.AreEqual(description, reader.Metadata.Description);
            var sum = 0L;
            using (Benchmark.Run("Read", count))
            {
                foreach (var keyValuePair in reader)
                {
                    sum += keyValuePair.Value.Value;
                    // Console.WriteLine($"{keyValuePair.Key} - {keyValuePair.Value.Timestamp} - {keyValuePair.Value.Value}");
                }
            }

            writer.Dispose();
            reader.Dispose();
        }
    }
}
