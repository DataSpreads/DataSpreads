﻿/*
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
using Spreads.Threading;
using System;
using System.Collections.Generic;
using Spreads.LMDB;

#pragma warning disable 618

namespace DataSpreads.Tests.Buffers
{
    [Category("CI")]
    [TestFixture]
    public class BufferRefTests
    {
        [Test]
        public void CouldSetUnsetFlag()
        {
            var br = BufferRef.Create(2, 1);
            var brFlag = br.SetFlag();
            Assert.AreEqual(2, brFlag.BucketIndex);
            Assert.AreEqual(1, brFlag.BufferIndex);
            Assert.IsTrue(brFlag.Flag);
            brFlag = br.ClearFlag();
            Assert.AreEqual(2, brFlag.BucketIndex);
            Assert.AreEqual(1, brFlag.BufferIndex);
            Assert.IsFalse(brFlag.Flag);
        }
    }
}
