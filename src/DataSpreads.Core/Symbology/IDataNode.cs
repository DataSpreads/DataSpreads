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

using System.Collections.Generic;

namespace DataSpreads.Symbology
{
    // Requirements:
    // Root DataStore globally is equivalent of org/user name. Individual users have prefix @i, e.g.  @i/buybackoff/
    // Non-default DataStores are children of the root node.
    // DataStores could have any number of root repos.
    // Repos are units of sharing. All content of repos has same access rights as the repo itself.
    //   Since root repos contain all data obviously we need to support that child repos have different access rights.
    //   By default they should inherit from parent or root? For simplicity we may just use whitelist and union it down the path,
    //   e.g. root/org/clients/public/. Clients is a separate lookup, org is lookup on accessor. Public requires no checks.
    // Data/access modifications require crypto-signed transactions? But it is built-in in the data definition event stream.
    // DataStores and Repo have UUID and could be renamed. Global mapping depends on UUIDs, global sid must remain the same after rename.
    // Assets (stream/containers/etc) cannot be moved outside. External dependencies could only be global.
    // Repos cannot move between data stores.

    // DataStore table
    // * LocalId -
    // * GlobalId - TODO we probably do not want to expose this and this could be mapped per session, which is probably simpler than global synchronization
    // * UUID - 16 bytes
    // * Name - null for default one, unique
    // * Version - last event number applied to the DataStore
    // * Hash of last event - we must check integrity, probably read first, calculate new hash from new event payload and compare to the new event hash. Throw if they are not equal.

    // DataNode mutation event must have
    // DataStore UUID - StreamId
    // Event type
    // Payload

    public enum DataNodeType
    {
        DataStore = 0,
        Repo = 1,
        Stream = 2,
        // Container,
        // Etc TODO tree structure could also be recursive/directional graph
    }

    public interface IDataNode
    {
        DataNodeType Type { get; }

        // Repo ParentRepo { get; } // TODO review. Methods are actually on Repo

        string FullPath { get; }

        string Name { get; }

        IDataNode Parent { get; }

        IEnumerable<IDataNode> Children { get; }
    }
}
