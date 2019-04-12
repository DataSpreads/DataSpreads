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
using Spreads;
using Spreads.DataTypes;
using Spreads.Threading;
using System;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Runtime.CompilerServices;
using System.Threading;
using Unsafe = System.Runtime.CompilerServices.Unsafe;

namespace DataSpreads.StreamLogs
{
    // TODO remove commented old methods

    internal readonly unsafe partial struct StreamLogState
    {
        #region Packer

        // We do not care who is doing packer work, it expires in 60 seconds if packer is inactive.
        /// <summary>
        /// Returns Timestamp of the lock or zero on failure to acquire lock.
        /// </summary>
        /// <param name="prolongExisting">Previous Timestamp. If it matches the current one then the lock is prolonged. Normally only lock holder knows the current value.</param>
        /// <returns></returns>
        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal long AcquirePackerLock(long prolongExisting = 0)
        {
            if (AdditionalCorrectnessChecks.Enabled)
            { AssertValidPointer(); }

            var existing = (Timestamp)Unsafe.ReadUnaligned<long>(_statePointer + StreamLogStateRecord.PackerTimestampOffset);
            var now = ProcessConfig.CurrentTime;

            if (prolongExisting == 0 && (now - existing).TimeSpan < TimeSpan.FromSeconds(StartupConfig.PackerTimeoutSeconds))
            {
                return 0;
            }

            var comparand = prolongExisting == 0 ? (long)existing : prolongExisting;

            var existing1 = Interlocked.CompareExchange(
                ref *(long*)(_statePointer + StreamLogStateRecord.PackerTimestampOffset), (long)now, comparand);
            if (existing1 == comparand)
            {
                return (long)now;
            }

            return 0;
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal bool ReleasePackerLock(long existing)
        {
            var existing1 = Interlocked.CompareExchange(
                ref *(long*)(_statePointer + StreamLogStateRecord.PackerTimestampOffset), 0L, existing);
            if (existing1 == existing)
            {
                return true;
            }

            return false;
        }

        #endregion Packer

        #region Lock

        internal Wpid LockHolder
        {
            [DebuggerStepThrough]
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                if (AdditionalCorrectnessChecks.Enabled)
                { AssertValidPointer(); }

                return SharedSpinLock.GetLockHolder(ref *(long*)(_statePointer + StreamLogStateRecord.LockerOffset));
            }
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryAcquireExclusiveLock(Wpid wpid, IWpidHelper wpidHelper, out byte threadToken, int spinLimit = 0)
        {
            var res =  SharedSpinLock.TryAcquireExclusiveLock(ref *(long*)(_statePointer + StreamLogStateRecord.LockerOffset), wpid, out threadToken, spinLimit, wpidHelper);
            return res;
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryReEnterExclusiveLock(Wpid wpid, IWpidHelper wpidHelper, byte threadToken, int spinLimit = 0)
        {
            var holder = LockHolder;

            return SharedSpinLock.TryReEnterExclusiveLock(ref *(long*)(_statePointer + StreamLogStateRecord.LockerOffset), wpid, threadToken, spinLimit, wpidHelper);
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryExitExclusiveLock(Wpid wpid, byte threadToken)
        {
            return SharedSpinLock.TryExitExclusiveLock(ref *(long*)(_statePointer + StreamLogStateRecord.LockerOffset), wpid, threadToken);
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryAcquireLock(Wpid wpid, IWpidHelper wpidHelper, int spinLimit = 0)
        {
            var content = *(long*) (_statePointer);
            *(long*) (_statePointer) = 0;
            return SharedSpinLock.TryAcquireLock(ref *(long*)(_statePointer + StreamLogStateRecord.LockerOffset), wpid, spinLimit, wpidHelper);
        }

        [Pure]
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public Wpid TryReleaseLock(Wpid wpid)
        {
            return SharedSpinLock.TryReleaseLock(ref *(long*)(_statePointer + StreamLogStateRecord.LockerOffset), wpid);
        }

        #endregion Lock

        
    }
}
