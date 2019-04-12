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

using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using DataSpreads.Config;
using DataSpreads.Storage;
using DataSpreads.StreamLogs;
using DataSpreads.Symbology;
using NUnit.Framework;
using Spreads;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.Utils;

namespace DataSpreads.Tests.Storage
{
    [Category("CI")]
    [TestFixture, SingleThreaded]
    public class SqLiteStorageMetadata
    {
        [Test]
        public void CouldOpenCloseStorage()
        {
            var path = TestUtils.GetPath(clear: true);
            var storage = new SQLiteStorage($@"Filename={Path.Combine(path, "metadatastorage.db")}");
            storage.Dispose();
        }


        [Test]
        public void CouldInsertAndSelectMetadata()
        {
            var path = TestUtils.GetPath(clear: true);
            var storage = new SQLiteStorage($@"Filename={Path.Combine(path, "metadatastorage.db")}");

            var md1 = new MetadataRecord()
            {
                PK = "test1:/md1",
                Metadata = new Metadata("md1"),
                RepoId = 2,
                ContainerId = 1
            };

            var md2 = new MetadataRecord()
            {
                PK = "test1:/md2",
                Metadata = new Metadata("md2"),
                RepoId = 2,
                ContainerId = 2
            };

            var txn = storage.BeginConcurrent();

            var result = storage.InsertMetadataRow(md1);
            Assert.IsTrue(result);
            result = storage.InsertMetadataRow(md2);
            Assert.IsTrue(result);
            result = storage.InsertMetadataRow(md2);
            Assert.IsFalse(result);

            txn.RawCommit();

            var mdList = storage.FindMetadata("test1:/md");

            Assert.AreEqual(2, mdList.Count);
            Assert.AreEqual("md1", mdList[0].Metadata.Description);
            Assert.AreEqual(2, mdList[0].RepoId);
            Assert.AreEqual(1, mdList[0].ContainerId);

            var mdExact = storage.GetExactMetadata("test1:/md2");
            Assert.AreEqual("md2", mdExact.Metadata.Description);

            storage.Dispose();
        }

        [Test]
        public void CouldIncrementCounter()
        {
            var path = TestUtils.GetPath(clear: true);
            var storage = new SQLiteStorage($@"Filename={Path.Combine(path, "metadatastorage.db")}");

            var md1 = new MetadataRecord()
            {
                PK = "test1:/md1",
                Metadata = new Metadata("md1"),
                RepoId = 2,
                ContainerId = 1
            };

            var insertResult = storage.InsertMetadataRow(md1);
            Assert.IsTrue(insertResult);

            var result = storage.IncrementCounter("test1:/md1");
            var result1 = storage.IncrementCounter("test1:/md1");
            Assert.AreEqual(result1, result + 1);

            var mdExact = storage.GetExactMetadata("test1:/md1");
            Assert.AreEqual(result1, mdExact.Counter);
            storage.Dispose();
        }
    }
}
