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
using System.Threading.Tasks;
using DataSpreads.Symbology;
using NUnit.Framework;

namespace DataSpreads.Tests.Symbology
{
    [TestFixture]
    public class MetadataManagerTests
    {
        [Test]
        public void CouldOpenCloseMetadataManager()
        {
            var path = TestUtils.GetPath(clear: true);
            for (int i = 0; i < 5; i++)
            {
                var mdm = new MetadataManager(path);
                mdm.Dispose();
            }
        }

        [Test]
        public async Task CouldGetRootRepoByPathAsync()
        {
            var path = TestUtils.GetPath(clear: true);
            var mdm = new MetadataManager(path);
            var rr = await mdm.GetMetadataRecordByPathAsync(new DataPath("~/"));
            Assert.AreEqual(1, rr.RepoId);
        }

        [Test]
        public async Task CouldCreateRepoAndGetItByPathAsync()
        {
            var path = TestUtils.GetPath(clear: true);
            var mdm = new MetadataManager(path);
            
            var newRepoRecord = await mdm.CreateRepoAsync(new DataPath("~/test/"));
            Assert.IsTrue(newRepoRecord.PK != null);

            var repo1 = await mdm.GetMetadataRecordByPathAsync(new DataPath("~/test/"));
            Assert.AreEqual(1, repo1.ParentId);

            newRepoRecord = await mdm.CreateRepoAsync(new DataPath("~/test/test/"));
            Assert.IsTrue(newRepoRecord.PK != null);

            var repo2 = await mdm.GetMetadataRecordByPathAsync(new DataPath("~/test/test/"));
            Assert.AreEqual(repo1.RepoId, repo2.ParentId);
        }

        [Test]
        public async Task CouldCreateLocalRepoAndGetItByPathAsync()
        {
            var path = TestUtils.GetPath(clear: true);
            var mdm = new MetadataManager(path);

            var newRepoRecord = await mdm.CreateRepoAsync(new DataPath("_/test/"));
            Assert.IsTrue(newRepoRecord.PK != null);

            var repo1 = await mdm.GetMetadataRecordByPathAsync(new DataPath("_/test/"));
            Assert.AreEqual(-1, repo1.ParentId);
            Assert.AreEqual(-2, repo1.RepoId);

            newRepoRecord = await mdm.CreateRepoAsync(new DataPath("_/test/test/"));
            Assert.IsTrue(newRepoRecord.PK != null);

            var repo2 = await mdm.GetMetadataRecordByPathAsync(new DataPath("_/test/test/"));
            Assert.AreEqual(repo1.RepoId, repo2.ParentId);
            Assert.IsTrue(repo2.RepoId == -3);
        }

        [Test]
        public async Task CannotCreateStreamWithTheSameNameAsRepoAsync()
        {
            var path = TestUtils.GetPath(clear: true);
            var mdm = new MetadataManager(path);

            var record = await mdm.CreateRepoAsync(new DataPath("~/test/"));
            Assert.IsTrue(record.PK != null);

            var repo1 = await mdm.GetMetadataRecordByPathAsync(new DataPath("~/test/"));
            Assert.AreEqual(1, repo1.ParentId);

            record = await mdm.CreateStreamAsync(new DataPath("~/test"), new Metadata(""), default);
            Assert.IsFalse(record.PK != null);

           
        }


        [Test]
        public async Task CouldCreateRepoAsync()
        {
            var path = TestUtils.GetPath(clear: true);
            var mdm = new MetadataManager(path);

            var dataPath = new DataPath("~/test/");
            var testRow = await mdm.CreateRepoAsync(dataPath);

            var testRow1 = await mdm.CreateRepoAsync(dataPath);
            Assert.IsNull(testRow1.PK);

            Assert.IsTrue(testRow.PK != null);
            Console.WriteLine(testRow.PK);
            Assert.AreEqual(1, testRow.ParentId);

            for (int i = 0; i < 100; i++)
            {
                dataPath = new DataPath(dataPath + $"sub{i}/");
                var testRecordSub = await mdm.CreateRepoAsync(dataPath);

                Assert.IsTrue(testRecordSub.PK != null);
                Console.WriteLine(i + " - " + testRecordSub.PK.Length + ": " + testRecordSub.PK);
                Assert.AreEqual(testRow.RepoId, testRecordSub.ParentId);
                testRow = testRecordSub;

                var getRepo = await mdm.GetMetadataRecordByPathAsync(dataPath);
                Assert.IsTrue(getRepo.PK != null);
            }

            mdm.Dispose();
        }

        [Test]
        public async Task CouldCreateStreamAsync()
        {
            var path = TestUtils.GetPath(clear: true);
            var mdm = new MetadataManager(path);

            var dataPath = new DataPath("~/test/");
            var testRow = await mdm.CreateRepoAsync(dataPath);

            var testRow1 = await mdm.CreateRepoAsync(dataPath);
            Assert.IsNull(testRow1.PK);

            Assert.IsTrue(testRow.PK != null);
            Console.WriteLine(testRow.PK);
            Assert.AreEqual(1, testRow.ParentId);

            for (int i = 0; i < 100; i++)
            {
                var streamDataPath = new DataPath(dataPath + $"name_of.some_stream{i}");
                var testRowStream = await mdm.CreateStreamAsync(streamDataPath, new Metadata(""), default);

                Assert.IsTrue(testRowStream.PK != null);
                Console.WriteLine(i + " - " + testRowStream.PK.Length + ": " + testRowStream.ContainerId + ": " + testRowStream.PK);
                Assert.AreEqual(testRow.RepoId, testRowStream.ParentId);

                var getStream = await mdm.GetMetadataRecordByPathAsync(streamDataPath);
                Assert.IsTrue(getStream.PK != null);

            }

            mdm.Dispose();
        }
    }
}
