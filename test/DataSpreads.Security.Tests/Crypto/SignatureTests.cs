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
using System.Linq;
using System.Security.Cryptography;
using DataSpreads.Security.Crypto;
using NSec.Cryptography;
using NUnit.Framework;
using Org.BouncyCastle.Crypto.Digests;
using Spreads.Utils;
using HashAlgorithm = NSec.Cryptography.HashAlgorithm;

namespace DataSpreads.Tests.Crypto
{
    [TestFixture]
    public class SignatureTests
    {
        [Test]
        public void CouldRoundtripToString()
        {
            var h1 = Signature64.Random();
            var s1 = h1.ToString();
            Console.WriteLine(s1);
            var h2 = new Signature64(s1);
            var s2 = h2.ToString();
            Assert.AreEqual(s1, s2, "strings equal");
            Assert.AreEqual(h1, h2, "hashes equal");
            Assert.AreEqual(h1.ToString(), s2);
        }
    }
}
