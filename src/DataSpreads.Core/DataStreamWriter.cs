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

using DataSpreads.Config;
using DataSpreads.StreamLogs;
using Spreads;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.Serialization;
using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace DataSpreads
{
    /// <summary>
    /// Writes new values to a data stream and holds an exclusive lock until disposed.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DataStreamWriter<T> : IDisposable
    {
        // TODO GetStream method on writer and GetWriter method on DS

        internal readonly StreamLog Inner;

        private readonly SerializationFormat _format; // no compression here

        internal readonly bool NoTimestamp;
        internal KeySorting EnforceTimestampSorting;
        private GCHandle _handle;
        private KeySorting _trackedKeySorting;
        private byte _threadToken;

        public ulong LastVersion => throw new NotImplementedException(); // from SL active chunk if not completed

        public Timestamp LastTimestamp => throw new NotImplementedException(); // from SL active chunk if not completed

        public KeySorting TimestampSorting => throw new NotImplementedException(); // from state

        public string TextId
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => Inner.TextId;
        }

        internal DataStreamWriter(StreamLog inner, KeySorting enforceTimestampSorting, WriteMode writeMode)
        {
            BinarySerializer.WarmUp<T>();

            var acquired = inner.TryAcquireExlusiveLock(out var tt);
            if (acquired != 0)
            {
                SingleWriterException.Throw($"Data stream {inner.TextId} is already open for write. Only one writer instance is currently supported.");
            }

            _threadToken = tt;

            inner.State.SetWriteMode(writeMode);
            inner.WriteMode = writeMode;

            _trackedKeySorting = inner.State.GetTrackedKeySorting();

            Inner = inner;
            // TODO Review this is quite expensive in hot loops, but opening streams is not hot.
            // But this is only to overcome GC non-determinism since SL is only weakly referenced
            // from SLM. If DSW is dropped without dispose then SL could be finalized before DSW
            // and it will FailFast because the exclusive lock is not released.
            _handle = GCHandle.Alloc(inner, GCHandleType.Normal);

            if (Inner.StreamLogFlags.IsBinary())
            {
                _format = SerializationFormat.Binary;
            }
            else
            {
                _format = SerializationFormat.Json;
            }

            NoTimestamp = Inner.StreamLogFlags.NoTimestamp();
            EnforceTimestampSorting = enforceTimestampSorting;
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal ValueTask<bool> TryAppendImplNoTimestamp(ulong version, T value, Timestamp timestamp)
        {
            DirectBuffer claimDb;

            var size = BinarySerializer.SizeOf(in value, out var tempStream, _format) - DataTypeHeader.Size;

            claimDb = Inner.LockAndClaim(version, size, _threadToken);
            if (!claimDb.IsValid)
            {
                return new ValueTask<bool>(false);
            }

            var written = BinarySerializer.Write(in value, ref Inner.ItemDth, claimDb, tempStream, _format);
            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (written != size)
                {
                    FailFastBadWrittenLength();
                }
            }

            var commitVersion = Inner.CommitAndRelease(_threadToken);

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (version != 0 && version != commitVersion)
                {
                    FailFastBadVersion();
                }
            }

            return Inner.WriteMode.HasLocalAck()
                ? WaitForAckBool(version)
                : new ValueTask<bool>(true);
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal ValueTask<bool> TryAppendImpl(ulong version, T value, Timestamp timestamp)
        {
            DirectBuffer claimDb;

            var size = BinarySerializer.SizeOf(in value, out var tempStream, _format) - DataTypeHeader.Size;

            if (NoTimestamp)
            {
                claimDb = Inner.Claim(version, size);
                if (!claimDb.IsValid)
                {
                    return new ValueTask<bool>(false);
                }
            }
            else
            {
                claimDb = Inner.LockAndClaim(version, Timestamp.Size + size, _threadToken);
                if (!claimDb.IsValid)
                {
                    return new ValueTask<bool>(false);
                }

                if (EnforceTimestampSorting != KeySorting.NotEnforced)
                {
                    var lastDb = Inner.DangerousGetUnpacked(version);
                    var lastTs = lastDb.Read<Timestamp>(0);
                    if ((EnforceTimestampSorting == KeySorting.Strong && timestamp <= lastTs)
                        || (EnforceTimestampSorting == KeySorting.Weak && timestamp < lastTs))
                    {
                        Inner.AbortValidClaimAndRelease(_threadToken);
                        return new ValueTask<bool>(false);
                    }
                }

                claimDb.Write(0, timestamp);
                claimDb = claimDb.Slice(Timestamp.Size);
            }

            var written = BinarySerializer.Write(in value, ref Inner.ItemDth, claimDb, tempStream, _format);
            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (written != size)
                {
                    FailFastBadWrittenLength();
                }
            }

            var commitVersion = Inner.CommitAndRelease(_threadToken);

            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (version != 0 && version != commitVersion)
                {
                    FailFastBadVersion();
                }
            }

            return Inner.WriteMode.HasLocalAck()
                ? WaitForAckBool(version)
                : new ValueTask<bool>(true);
        }

        //        [MethodImpl(MethodImplOptions.NoInlining
        //#if NETCOREAPP
        //                    | MethodImplOptions.AggressiveOptimization
        //#endif
        //        )]
        //        internal ValueTask<bool> TryAppendImplContinue(ulong version, T value, Timestamp timestamp, DirectBuffer claimDb, BufferWriter payload, int size)
        //        {
        //            var written = BinarySerializer.Write(in value, ref Inner.ItemDth, claimDb, payload, _format);
        //            if (AdditionalCorrectnessChecks.Enabled)
        //            {
        //                if (written != size)
        //                {
        //                    FailFastBadWrittenLength();
        //                }
        //            }

        //            var commitVersion = Inner.CommitAndRelease();

        //            if (AdditionalCorrectnessChecks.Enabled)
        //            {
        //                if (version != 0 && version != commitVersion)
        //                {
        //                    FailFastBadVersion();
        //                }
        //            }

        //            return Inner.WriteMode.HasLocalAck()
        //                ? WaitForAckBool(version)
        //                : new ValueTask<bool>(true);
        //        }

        #region TryAppendImpl without version

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal ValueTask<ulong> TryAppendImpl(T value, Timestamp timestamp)
        {
            var size = BinarySerializer.SizeOf(in value, out var payload, _format) - DataTypeHeader.Size;

            var claimDb = Inner.LockAndClaim(0, Timestamp.Size + size, _threadToken);

            if (!claimDb.IsValid)
            {
                return new ValueTask<ulong>(0);
            }

            if (Inner.ActiveBlock.CurrentVersion > 0)
            {
                var activeBlockCount = Inner.ActiveBlock.Count;
                Timestamp lastTs;
                if (activeBlockCount > 0)
                {
                    lastTs = Inner.ActiveBlock.DangerousGetTimestampAtIndex(activeBlockCount - 1);
                }
                else
                {
                    lastTs = Inner.ActiveBlock.PreviousLastTimestamp;
                }

                var tsc = KeyComparer<Timestamp>.Default.Compare(timestamp, lastTs);
                if (EnforceTimestampSorting != KeySorting.NotEnforced)
                {
                    if ((EnforceTimestampSorting == KeySorting.Strong && tsc <= 0)
                        || (EnforceTimestampSorting == KeySorting.Weak && tsc < 0))
                    {
                        Inner.AbortValidClaimAndRelease(_threadToken);
                        return new ValueTask<ulong>(0);
                    }
                }

                if (tsc > 0 || _trackedKeySorting == KeySorting.NotEnforced)
                {
                    // do nothing, leave tracked sorting as is
                }
                else if (_trackedKeySorting == KeySorting.Strong)
                {
                    if (tsc == 0)
                    {
                        _trackedKeySorting = KeySorting.Weak;
                        Inner.State.SetTrackedKeySorting(KeySorting.Weak);
                    }
                    else
                    {
                        Debug.Assert(tsc < 0);
                        _trackedKeySorting = KeySorting.NotEnforced;
                        Inner.State.SetTrackedKeySorting(KeySorting.NotEnforced);
                    }
                }
                else if (_trackedKeySorting == KeySorting.Weak && tsc < 0)
                {
                    _trackedKeySorting = KeySorting.NotEnforced;
                    Inner.State.SetTrackedKeySorting(KeySorting.NotEnforced);
                }
            }

            claimDb.Write(0, timestamp);

            claimDb = claimDb.Slice(Timestamp.Size);

            return TryAppendImplContinue(in value, in claimDb, payload, size);
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP3_0
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        internal ValueTask<ulong> TryAppendImplNoTimestamp(T value)
        {
            var size = BinarySerializer.SizeOf(in value, out var payload, _format) - DataTypeHeader.Size;

            var claimDb = Inner.Claim(0, size);
            if (!claimDb.IsValid)
            {
                return new ValueTask<ulong>(0);
            }

            return TryAppendImplContinue(in value, in claimDb, payload, size);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ValueTask<ulong> TryAppendImplContinue(in T value, in DirectBuffer claimDb, BufferWriter payload, int size)
        {
            var written = BinarySerializer.Write(in value, ref Inner.ItemDth, claimDb, payload, _format);
            if (AdditionalCorrectnessChecks.Enabled)
            {
                if (written != size)
                {
                    FailFastBadWrittenLength();
                }
            }

            var version = Inner.CommitAndRelease(_threadToken);

            return Inner.WriteMode.HasLocalAck()
                ? new ValueTask<ulong>(Inner.WaitForAck(version), 0)
                : new ValueTask<ulong>(version);
        }

        #endregion TryAppendImpl without version

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Task Complete()
        {
            return Inner.Complete();
        }

        public bool TrySyncRemote()
        {
            throw new NotImplementedException();
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailFastBadWrittenLength()
        {
            ThrowHelper.FailFast("written != sizeOf");
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void FailFastBadVersion()
        {
            ThrowHelper.FailFast("version != commitVersion");
        }

        protected async ValueTask<bool> WaitForAckBool(ulong version)
        {
            await new ValueTask<ulong>(Inner.WaitForAck(version), 0);
            return true;
        }

        private void Dispose(bool disposing)
        {
            if (!disposing)
            {
                Trace.TraceWarning($"Finalizing writer for data stream {TextId}.");
            }

            Inner.State.SetWriteMode(default);
            Inner.WriteMode = default;
            var released = Inner.TryReleaseExclusiveLock();
            if (released != 0)
            {
                ThrowHelper.FailFast("Cannot release lock");
            }
            _handle.Free();
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        ~DataStreamWriter()
        {
            Dispose(false);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining
#if NETCOREAPP
        | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public ValueTask<ulong> TryAppend(T value)
        {
            // This if block inside TryAppendImpl significantly reduces performance
            // probably because it breaks some optimizations. The methods
            // below are marked with NoInlining so this public method is
            // quite small and does dispatching depending on _noTimestamp.
            // True value should be rare hence this should be predicted well.
            // Marking this method with AggressiveInlining does not improve
            // performance even on micro benchmarks regardless of using await or .Result.

            // ReSharper disable once ConvertIfStatementToReturnStatement
            if (NoTimestamp)
            {
                return TryAppendImplNoTimestamp(value);
            }
            else
            {
                return TryAppendImpl(value, ProcessConfig.CurrentTime);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public ValueTask<bool> TryAppend(ulong version, T value)
        {
            return TryAppendImpl(version, value, ProcessConfig.CurrentTime);
        }

        /// <summary>
        /// Returns a data stream writer that accepts user-provided timestamps for values.
        /// </summary>
        /// <param name="enforceTimestampSorting">
        /// Require that a timestamp of a new value is greater than
        /// (for <see cref="KeySorting.Strong"/>) or greater or equal
        /// (for <see cref="KeySorting.Weak"/>) than the timestamp
        /// of the last value in the data stream. When key sorting
        /// is not enforced (default) it is still tracked.
        /// </param>
        /// <returns></returns>
        public DataStreamWriterWithTimestamp<T> WithTimestamp(KeySorting enforceTimestampSorting = KeySorting.NotEnforced)
        {
            return new DataStreamWriterWithTimestamp<T>(this, enforceTimestampSorting);
        }
    }

    public readonly struct DataStreamWriterWithTimestamp<T> : IDisposable
    {
        private readonly DataStreamWriter<T> _inner;

        internal DataStreamWriterWithTimestamp(DataStreamWriter<T> inner, KeySorting enforceTimestampSorting)
        {
            _inner = inner;
            _inner.EnforceTimestampSorting = enforceTimestampSorting;
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public ValueTask<ulong> TryAppend(T value, Timestamp timestamp)
        {
            if (_inner.NoTimestamp)
            {
                ThrowHelper.FailFast("Must have thrown in ctor, WithTimestamp and noTimestamp are impossible together.");
            }

            return _inner.TryAppendImpl(value, timestamp);
        }

        [MethodImpl(MethodImplOptions.NoInlining
#if NETCOREAPP
                    | MethodImplOptions.AggressiveOptimization
#endif
        )]
        public ValueTask<bool> TryAppend(ulong version, T value, Timestamp timestamp)
        {
            return _inner.TryAppendImpl(version, value, timestamp);
        }

        public void Dispose()
        {
            _inner.Dispose();
        }
    }
}
