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

using DataSpreads.Symbology;
using NUnit.Framework;

namespace DataSpreads.Tests.Symbology
{
    [TestFixture]
    public class DataPathTests
    {
        [Test]
        public void CouldCreatePath()
        {
            var str = "a/b/c";
            var repoPath = new DataPath(str, false);
            Assert.AreEqual(str, repoPath.ToString());
            var streamPath = new DataPath(str, true);
            Assert.AreEqual(str + "/", streamPath.ToString());

            Assert.AreEqual("a/b/", repoPath.Parent.ToString());
            Assert.AreEqual("a/b/", streamPath.Parent.ToString());
            Assert.AreEqual("a/", streamPath.Parent.Parent.ToString());
            Assert.AreEqual("", streamPath.Parent.Parent.Parent.ToString());

            str = "~/a/b/c";
            repoPath = new DataPath(str, false);
            Assert.AreEqual(str, repoPath.ToString());
            streamPath = new DataPath(str, true);
            Assert.AreEqual(str + "/", streamPath.ToString());

            Assert.AreEqual("~/a/b/", repoPath.Parent.ToString());
            Assert.AreEqual("~/a/b/", streamPath.Parent.ToString());
            Assert.AreEqual("~/a/", streamPath.Parent.Parent.ToString());
            Assert.AreEqual("~/", streamPath.Parent.Parent.Parent.ToString());
            Assert.AreEqual("", streamPath.Parent.Parent.Parent.Parent.ToString());
        }

        [Test]
        public void CouldValidateDataPath()
        {
            Assert.IsTrue(DataPath.TryValidate("a/b/c", out _, out _));
            Assert.IsTrue(DataPath.TryValidate("_", out _, out _));
            Assert.IsTrue(DataPath.TryValidate("@a/b/", out _, out _));
            Assert.IsTrue(DataPath.TryValidate("/a/b/@c/", out var rewritten, out _));
            Assert.AreEqual("@c/a/b", rewritten);
            Assert.IsTrue(DataPath.TryValidate("@my/a/b/", out rewritten, out _));
            Assert.AreEqual("~/a/b", rewritten);
            Assert.IsTrue(DataPath.TryValidate("~/a/b/", out _, out _));
            Assert.IsTrue(DataPath.TryValidate("sdvFSbdvfju.eegb.ewg-weg.___degewbg.dfs", out _, out _));
           
            
            Assert.IsTrue(DataPath.TryValidate("a.b", out _, out _));

            Assert.IsFalse(DataPath.TryValidate("~a/b/", out _, out _));
            Assert.IsFalse(DataPath.TryValidate(".", out _, out _));
            Assert.IsFalse(DataPath.TryValidate("..", out _, out _));
            Assert.IsFalse(DataPath.TryValidate("-", out _, out _));
            Assert.IsFalse(DataPath.TryValidate("--", out _, out _));
        }
    }
}
