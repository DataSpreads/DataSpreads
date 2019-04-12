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
using DataSpreads.Symbology;
using DataSpreads.Symbology.Experimental;
using NUnit.Framework;

namespace DataSpreads.Tests.Symbology
{
    [Obsolete]
    [TestFixture]
    public class DataPathStructTestsObsolete
    {
        [Test]
        public void CouldCreatePath()
        {
            var str = "a/b/c";
            var p = new DataPathStruct(str);
            Assert.AreEqual(2, p.Level);
            Assert.AreEqual(5, p.Length);
            Assert.AreEqual(str, p.ToString());
            Assert.IsFalse(p.IsRepo);
            Assert.IsFalse(p.IsExternal);
        }

        [Test]
        public void CouldCreateRepoPath()
        {
            var str = "a/b/c/";
            var p = new DataPathStruct(str);
            Assert.AreEqual(3, p.Level);
            Assert.AreEqual(6, p.Length);
            Assert.AreEqual(str, p.ToString());
            Assert.IsTrue(p.IsRepo);
            Assert.IsFalse(p.IsExternal);
        }

        [Test]
        public void CouldCreateExternalRepoPath()
        {
            var str = "@a/b/c/";
            var p = new DataPathStruct(str);
            Assert.AreEqual(3, p.Level);
            Assert.AreEqual(7, p.Length);
            Assert.AreEqual(str, p.ToString());
            Assert.IsTrue(p.IsRepo);
            Assert.IsTrue(p.IsExternal);
        }

        [Test]
        public void CouldAppendPath()
        {
            var p = new DataPathStruct("a/b/c");
            var p2 = new DataPathStruct("d/e/f/g");
            var appended = p.Append(p2);
            Assert.AreEqual(p.Level + p2.Level + 1, appended.Level);
            Assert.AreEqual(6, appended.Level);
            Assert.AreEqual("a/b/c/d/e/f/g", appended.ToString());
        }

        [Test]
        public void CouldGetParent()
        {
            var p = new DataPathStruct("a/b/c");
            var parent = p.Parent();
            Assert.AreEqual("a/b/", parent.ToString());
            Assert.AreEqual(2, parent.Level);

            parent = parent.Parent();
            Assert.AreEqual("a/", parent.ToString());
            Assert.AreEqual(1, parent.Level);

            parent = parent.Parent();
            Assert.AreEqual("", parent.ToString());
            Assert.AreEqual(0, parent.Level);

            parent = new DataPathStruct("a").Parent();
            Assert.AreEqual("", parent.ToString());
            Assert.AreEqual(0, parent.Level);
        }
    }
}
