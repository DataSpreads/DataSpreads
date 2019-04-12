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

namespace DataSpreads.Symbology
{
    // TODO xml docs

    [Flags]
    internal enum Permissions
    {
        /// <summary>
        /// Read data.
        /// </summary>
        Read = 0b_0000_0000_0001,

        /// <summary>
        /// Write data.
        /// </summary>
        Write = 0b_0000_0000_0010,

        /// <summary>
        /// Create new streams in existing repos.
        /// </summary>
        CreateStream = 0b_0000_0001_0000,

        DeleteStream = 0b_0000_0010_0000,

        RenameStream = 0b_0000_0100_0000,

        CompleteStream = 0b_0000_1000_0000,

        CreateRepo = 0b_0001_0000_0000,
        DeleteRepo = 0b_0010_0000_0000,
        RenameRepo = 0b_0100_0000_0000,
        ArchiveRepo = 0b_1000_0000_0000,

        GrantPermissions = 0b_0001_0000_0000_0000,

        Own = 0b_0010_0000_0000_0000,
        TransferOwnership = 0b_0100_0000_0000_0000,

        // Presets

        Reader = Read,
        DataContributor = Read | Write,
        StreamManager = DataContributor | CreateStream | DeleteStream | RenameStream | CompleteStream,
        RepoManager = StreamManager | CreateRepo | DeleteRepo | RenameRepo | ArchiveRepo,
        Owner = RepoManager | GrantPermissions | Own,
        Super = Owner | TransferOwnership
    }
}
