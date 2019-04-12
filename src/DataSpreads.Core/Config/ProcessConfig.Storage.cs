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

using DataSpreads.Buffers;
using Spreads;
using Spreads.Buffers;
using Spreads.DataTypes;
using Spreads.LMDB;
using Spreads.Threading;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataSpreads.Config
{
    internal partial class ProcessConfig
    {
        [StructLayout(LayoutKind.Sequential)]
        internal readonly unsafe struct ProcessConfigRecord
        {
            public const int BufferSize = 32;

            internal readonly DirectBuffer _processBuffer;

            public ProcessConfigRecord(DirectBuffer processBuffer)
            {
                _processBuffer = processBuffer;
            }

            [MethodImpl(MethodImplOptions.NoInlining)]
            private void AssertValidPointer()
            {
                if (!_processBuffer.IsValid)
                {
                    ThrowHelper.ThrowInvalidOperationException("_processBuffer is invalid in ProcessRecord");
                }
            }

            //
            public const int WpidOffset = 0;

            public Wpid Wpid
            {
                [DebuggerStepThrough]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    AssertValidPointer();
                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                    return _processBuffer.Read<Wpid>(WpidOffset);
                }
            }

            //
            public const int TimeStampOffset = 8;

            internal IntPtr TimeStampPointer => (IntPtr)(_processBuffer.Data + TimeStampOffset);

            /// <summary>
            /// Shared <see cref="TimeService"/> value for shared record (zero instance id)
            /// or process heartbeat for process records.
            /// </summary>
            public Timestamp Timestamp
            {
                [DebuggerStepThrough]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    AssertValidPointer();
                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                    return (Timestamp)_processBuffer.VolatileReadInt64(TimeStampOffset);
                }
                [DebuggerStepThrough]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    AssertValidPointer();
                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                    _processBuffer.VolatileWriteInt64(TimeStampOffset, (long)value);
                }
            }

            // We do not clear dead Wpid buffer instantly. If the wpid hung
            // and then comes back it must see that it is assumed dead and clear itself.
            // Other processes will clear the page after some time.
            public const int AssumedDeadOffset = 16;

            public Timestamp AssumedDead
            {
                [DebuggerStepThrough]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get
                {
                    AssertValidPointer();
                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                    return (Timestamp)_processBuffer.VolatileReadInt64(AssumedDeadOffset);
                }
                [DebuggerStepThrough]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set
                {
                    AssertValidPointer();
                    // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                    _processBuffer.VolatileWriteInt64(AssumedDeadOffset, (long)value);
                }
            }

            public Timestamp CasAssumedDead(Timestamp newValue, Timestamp existing)
            {
                // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                return (Timestamp)_processBuffer.InterlockedCompareExchangeInt64(AssumedDeadOffset, (long)newValue, (long)existing);
            }

            public bool IsValid
            {
                [DebuggerStepThrough]
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => _processBuffer.IsValid;
            }

            // Used by shared record.
            public const int SharedInstanceIdCounterOffset = 24;

            //
            // ReSharper disable once UnusedMember.Local
            private const int ReservedIntOffset = 28;
        }

        [StructLayout(LayoutKind.Explicit, Size = 1024)]
        internal unsafe struct DataStoreRecord
        {
            [FieldOffset(0)]
            public readonly Symbol64 Name;

            [FieldOffset(64)]
            public readonly UUID UUID;

            // Version of data store event stream that modified this record.
            [FieldOffset(80)]
            public readonly ulong Version;

            [FieldOffset(88)]
            public readonly uint Id;

            [FieldOffset(92)]
            public fixed byte Reserved[164];

            [FieldOffset(256)]
            public readonly Symbol256 DataPath;

            [FieldOffset(512)]
            public readonly Symbol256 WalPath;

            [FieldOffset(768)]
            public readonly Symbol256 ArchivePath;
        }

        internal class ProcessConfigStorage : BufferRefAllocator
        {
            private readonly Database _processDb;
            private readonly SharedMemoryBuckets _buckets;

            // ReSharper disable once MemberHidesStaticFromOuterClass
            public ProcessConfigRecord SharedRecord { [MethodImpl(MethodImplOptions.AggressiveInlining)] get; }

            public ProcessConfigStorage(string directoryPath)
                : base(directoryPath, StartupConfig.ProcessConfigEnvFlags, default, ProcessConfigRecord.BufferSize,
                    1024L * 1024 * 1024) // 1GB is way too much but it takes no resources above actually used
            {
                // BRA dbs and _env are set in base class

                // InstanceId -> BufferRef
                _processDb = Environment.OpenDatabase("_processConfig",
                    new DatabaseConfig(DbFlags.Create | DbFlags.IntegerKey));

                _buckets = new SharedMemoryBuckets(directoryPath, ProcessConfigRecord.BufferSize, 0);

                using (var txn = Environment.BeginTransaction())
                {
                    try
                    {
                        uint sharedInstanceId = 0;

                        if (_processDb.TryGet(txn, ref sharedInstanceId, out BufferRef sharedBufferRef))
                        {
                            txn.Abort();
                        }
                        else
                        {
                            sharedBufferRef = Allocate(txn, 0, out var fromFreeList, null);
                            _processDb.Put(txn, sharedInstanceId, sharedBufferRef, TransactionPutOptions.NoOverwrite);
                            txn.Commit();
                            if (!fromFreeList)
                            {
                                Environment.Sync(true);
                            }
                        }

                        SharedRecord = new ProcessConfigRecord(_buckets[sharedBufferRef]);
                    }
                    catch (Exception ex)
                    {
                        txn.Abort();
                        Trace.TraceError(ex.ToString());
                        throw;
                    }
                }
            }

            /// <summary>
            /// Increment shared instance id and return a process buffer with new Wpid
            /// </summary>
            /// <returns></returns>
            public ProcessConfigRecord CreateNew()
            {
                using (var txn = Environment.BeginTransaction())
                {
                    while (true)
                    {
                        // ReSharper disable once ImpureMethodCallOnReadonlyValueField
                        var newInsatnceId = unchecked((uint)SharedRecord._processBuffer.InterlockedIncrementInt32(ProcessConfigRecord.SharedInstanceIdCounterOffset));
                        try
                        {
                            // We try to increment instance id instead of reuse existing
                            // At some point after running for very very long time it might overflow
                            // and start from zero again - this is fine.
                            if (_processDb.TryGet(txn, ref newInsatnceId, out BufferRef bufferRef))
                            {
                                continue;
                            }

                            bufferRef = Allocate(txn, 0, out var fromFreeList, null);
                            _processDb.Put(txn, newInsatnceId, bufferRef, TransactionPutOptions.NoOverwrite);
                            txn.Commit();
                            if (!fromFreeList)
                            {
                                Environment.Sync(true);
                            }

                            var result = _buckets[bufferRef];
                            if (fromFreeList)
                            {
                                // in Delete we clear the buffer after commit, there is small chance a process died after commit but before cleaning
                                result.Clear(0, result.Length);
                            }
                            else
                            {
                                Debug.Assert(result.IsFilledWithValue(0), "a new ProcessConfig buffer must be clean.");
                            }

                            result.WriteInt64(ProcessConfigRecord.WpidOffset, Wpid.Create(newInsatnceId));
                            var record = new ProcessConfigRecord(result);
                            return record;
                        }
                        catch (Exception ex)
                        {
                            txn.Abort();
                            Trace.TraceError(ex.ToString());
                            throw;
                        }
                    }
                }
            }

            public bool Delete(ProcessConfigRecord processConfigRecord)
            {
                if (!processConfigRecord.IsValid)
                {
                    return false;
                }
                using (var txn = Environment.BeginTransaction())
                {
                    var wpid = processConfigRecord.Wpid;
                    // Debug.Assert(!IsWpidAlive(processConfigRecord));

                    uint insatnceId = wpid.InstanceId;

                    try
                    {
                        // We try to increment instance id instead of reuse existing
                        // At some point after running for very very long time it might overflow
                        // and start from zero again - this is fine.
                        if (!_processDb.TryGet(txn, ref insatnceId, out BufferRef bufferRef))
                        {
                            // was already deleted
                            // TODO (?) throw? review
                            txn.Abort();
                            return false;
                        }

                        Free(txn, bufferRef);
                        _processDb.Delete(txn, insatnceId);
                        txn.Commit();

                        var db = _buckets[bufferRef];
                        db.Clear(0, db.Length);
                        return true;
                    }
                    catch (Exception ex)
                    {
                        txn.Abort();
                        Trace.TraceError(ex.ToString());
                        throw;
                    }
                }
            }

            // ReSharper disable once MemberHidesStaticFromOuterClass
            public ProcessConfigRecord GetRecord(Wpid wpid)
            {
                using (var txn = Environment.BeginReadOnlyTransaction())
                {
                    var insatnceId = wpid.InstanceId;

                    try
                    {
                        if (_processDb.TryGet(txn, ref insatnceId, out BufferRef bufferRef))
                        {
                            return new ProcessConfigRecord(_buckets[bufferRef]);
                        }
                    }
                    catch (Exception ex)
                    {
                        Trace.TraceError(ex.ToString());
                        throw;
                    }
                }
                return default;
            }

            internal List<Wpid> GetAllProcesses()
            {
                var list = new List<Wpid>();
                using (var txn = Environment.BeginReadOnlyTransaction())
                {
                    foreach (var keyValuePair in _processDb.AsEnumerable(txn))
                    {
                        var bufferRef = keyValuePair.Value.Read<BufferRef>(0);
                        var buffer = _buckets[bufferRef];
                        var wpid = buffer.Read<Wpid>(0);
                        list.Add(wpid);
                    }
                }

                return list;
            }

            public override void Dispose()
            {
                _processDb.Dispose();
                base.Dispose();
            }
        }
    }
}
