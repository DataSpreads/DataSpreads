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

using NSec.Cryptography;
using NUnit.Framework;
using Org.BouncyCastle.Crypto.Digests;
using Spreads.Utils;
using System;
using System.Linq;
using System.Security.Cryptography;
using HashAlgorithm = NSec.Cryptography.HashAlgorithm;

namespace DataSpreads.Tests.Utlis
{
    [TestFixture]
    public class CryptoTests
    {
        public byte[] CalculateHash(byte[] value)
        {
            var digest = new KeccakDigest(256);
            var output = new byte[digest.GetDigestSize()];
            digest.BlockUpdate(value, 0, value.Length);
            digest.DoFinal(output, 0);
            return output;
        }

        public static void Test(string sk, string pk, string msg, string sig)
        {
            var a = SignatureAlgorithm.Ed25519;

            Console.WriteLine(a.SignatureSize);
        }

        [Test]
        public void CouldHashSignVerify()
        {
            var a = SignatureAlgorithm.Ed25519;
            var b = HashAlgorithm.Blake2b_256;
            var testKey = Key.Create(a);

            var rng = new Random();
            var size = 100L;
            var bytes = new byte[size];
            var hash0 = new byte[32];
            var hash = new byte[32];
            var signature = new byte[64];

            rng.NextBytes(bytes);

            var count = 50_000;

            for (int i = 0; i < count / 10; i++)
            {
                SauceControl.Blake2Fast.Blake2b.ComputeAndWriteHash(32, bytes, hash0);
            }

            using (var stat = Benchmark.Run("SauceControl Hash (MBsec)", count * size, true))
            {
                for (int i = 0; i < count; i++)
                {
                    SauceControl.Blake2Fast.Blake2b.ComputeAndWriteHash(32, bytes, hash);
                }

                var throughput = (size * count * 1.0) / (stat.Stopwatch.ElapsedMilliseconds / 1000.0);
                Console.WriteLine("Throughput SauceControl: " + Math.Round(throughput / (1024 * 1024), 0));
            }
            Benchmark.Dump();

            using (var stat = Benchmark.Run("Hash (MBsec)", count * size, true))
            {
                for (int i = 0; i < count; i++)
                {
                    b.Hash(bytes, hash);
                }

                var throughput = (size * count * 1.0) / (stat.Stopwatch.ElapsedMilliseconds / 1000.0);
                Console.WriteLine("Throughput: " + Math.Round(throughput / (1024 * 1024), 0));
            }
            Benchmark.Dump();

            Assert.IsTrue(hash0.SequenceEqual(hash));

            using (var stat = Benchmark.Run("Sign (KOPS)", count * 1000, true))
            {
                for (int i = 0; i < count; i++)
                {
                    a.Sign(testKey, hash, signature);
                }
            }
            Benchmark.Dump();

            using (var stat = Benchmark.Run("Verify (KOPS)", count * 1000, true))
            {
                for (int i = 0; i < count; i++)
                {
                    if (!a.Verify(testKey.PublicKey, hash, signature))
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
            Benchmark.Dump();

            Console.WriteLine(a.SignatureSize);
            Console.WriteLine(b.HashSize);
        }

#if NETCOREAPP3_0

        [Test]
        public void DotNetAesTest()
        {
            // key should be: pre-known, derived, or transported via another channel, such as RSA encryption
            byte[] key = new byte[16];
            RandomNumberGenerator.Fill(key);

            byte[] nonce = new byte[12];
            RandomNumberGenerator.Fill(nonce);

            // normally this would be your data
            byte[] dataToEncrypt = new byte[1234];
            byte[] associatedData = new byte[333];
            RandomNumberGenerator.Fill(dataToEncrypt);
            RandomNumberGenerator.Fill(associatedData);

            // these will be filled during the encryption
            byte[] tag = new byte[16];
            byte[] ciphertext = new byte[dataToEncrypt.Length];

            using (AesGcm aesGcm = new AesGcm(key))
            {
                aesGcm.Encrypt(nonce, dataToEncrypt, ciphertext, tag, associatedData);
            }

            // tag, nonce, ciphertext, associatedData should be sent to the other part

            byte[] decryptedData = new byte[ciphertext.Length];

            using (AesGcm aesGcm = new AesGcm(key))
            {
                aesGcm.Decrypt(nonce, ciphertext, tag, decryptedData, associatedData);
            }

            // do something with the data
            // this should always print that data is the same
            Console.WriteLine($"AES-GCM: Decrypted data is {(dataToEncrypt.SequenceEqual(decryptedData) ? "the same as" : "different than")} original data.");
        }
#endif
    }
}
