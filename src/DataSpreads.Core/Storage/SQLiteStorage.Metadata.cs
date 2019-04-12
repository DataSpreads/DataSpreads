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

using Spreads;
using Spreads.SQLite;
using Spreads.SQLite.Fast;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using DataSpreads.Symbology;
using Spreads.Serialization;
using Spreads.Serialization.Utf8Json;
using Spreads.Serialization.Utf8Json.Resolvers.Internal;

namespace DataSpreads.Storage
{
    internal interface IMetadataStorage
    {
        /// <summary>
        /// Metadata storage is a single instance per <see cref="DataStore"/>.
        /// </summary>
        DataStore DataStore { get; }

        // Q: Add repos by path or to parent?
        // A: By parent.

        

        // Permissions:
        // Each repo should have a special sub-repo with permissions per user
        // Q: what to do with permission per group/org?
        // Q: permission is only set by repo owners, so this could be a single
        //    stream but every update to it is translated to user permissions streams
        //    So we need user permissions table: user -> repo -> permission
        //    Repos without permission behave like non-existing.
        //    Repo owner must have repo -> user -> permission, stored only in
        //    owner DStore and on the server.
        //    When User->Repo is empty we must re-sync.
        //    We should store that as SQL/LMDB streams, StreamLogs are too heavy for that.

        // User repos in teams/orgs:
        // Every user in a team has user@team/ repo with full write access and read visibility
        // to all other team members.

        // User could be a member of several teams.

        // Org could have multiple teams: @org.group.team.

        // User could be a member of multiple teams in org:
        //  user@org - if a user is a member of org
        //  user@org.team1 - user could be a member of a team but not top-level org

        // Implication:
        // Membership table per org:
        // User -> Group
    }

    // TODO have started somewhere similar enumeration, sync
    internal enum MetadataTypeX : byte
    {
        Account = 1,
        Repo = 2,
        Stream = 3
    }

    // ReSharper disable once InconsistentNaming
    internal partial class SQLiteStorage
    {
        private const string MetadataTable = "metadata_table";

        private static void CreateMetadataTables(SqliteConnection connection)
        {
            string commandText;
            SqliteCommand command;
            commandText =
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                $"CREATE TABLE IF NOT EXISTS `{MetadataTable}` (\n" +
                "  `PK`                VARCHAR(255) NOT NULL COLLATE NOCASE,\n" +
                "  `RepoId`            INTEGER DEFAULT 0,\n" +
                "  `ContainerId`          INTEGER DEFAULT 0,\n" +
                "  `ParentId`          INTEGER DEFAULT 0,\n" + // this depends on context: for repos it's repo or root, for streams it could be another stream
                "  `Counter`           INTEGER DEFAULT 0,\n" + // for repos number of data items
                "  `Version`           INTEGER DEFAULT 0,\n" +
                "  `RemoteId`          INTEGER DEFAULT 0,\n" +
                "  `Permissions`       INTEGER DEFAULT 0,\n" +
                "  `SchemaType`        INTEGER DEFAULT 0,\n" +
                "  `Schema`            TEXT DEFAULT NULL,\n" +
                "  `Metadata`          TEXT DEFAULT NULL,\n" +
                "  CONSTRAINT `UX_RepoContainerId` UNIQUE (`RepoId`, `ContainerId`)\n" +
                "  PRIMARY KEY (`PK`));\n";

#pragma warning restore HAA0201 // Implicit string concatenation allocation

            command = connection.CreateCommand();
            command.CommandText = commandText;
            command.ExecuteNonQuery();
            command.Dispose();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public List<MetadataRecord> FindMetadata(string path)
        {
            var qs = RentQueries();
            try
            {
                return qs.FindMetadata(path);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public MetadataRecord GetExactMetadata(string path)
        {
            var qs = RentQueries();
            try
            {
                return qs.GetExactMetadata(path);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int IncrementCounter(string path)
        {
            var qs = RentQueries();
            try
            {
                return qs.IncrementCounter(path);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool InsertMetadataRow(MetadataRecord record)
        {
            var qs = RentQueries();
            try
            {
                return qs.InsertMetadataRow(record);
            }
            finally
            {
                ReturnQueries(qs);
            }
        }

        public partial class StorageQueries
        {
            internal FastQuery InsertMetadataRowQuery;
            internal FastQuery FindMetadataQuery;
            internal FastQuery GetExactMetadataQuery;
            internal FastQuery IncrementCounterQuery;

            private void CreateMetadataQueries(SqliteConnection connection, ConnectionPool pool)
            {
                InsertMetadataRowQuery = CreateInsertMetadataRowQuery(connection, pool);
                FindMetadataQuery = CreateFindMetadataQuery(connection, pool);
                GetExactMetadataQuery = CreateGetExactMetadataQuery(connection, pool);
                IncrementCounterQuery = CreateIncrementCounterQuery(connection, pool);
            }

            private void DisposeMetadataQueries()
            {
                InsertMetadataRowQuery.Dispose();
                FindMetadataQuery.Dispose();
                GetExactMetadataQuery.Dispose();
                IncrementCounterQuery.Dispose();
            }

            private static MetadataRecord MapMetadataTableRow(QueryReader reader)
            {
                var pk = PtrToStringUTF8(reader.ColumnText(0));
                var repoId = checked((int)reader.ColumnInt64(1));
                var containerId = checked((int)reader.ColumnInt64(2));
                var parentId = checked((int)reader.ColumnInt64(3));
                var counter = checked((int)reader.ColumnInt64(4));
                var version = checked((int)reader.ColumnInt64(5));
                var remoteId = checked((int)reader.ColumnInt64(6));
                var permissions = checked((int)reader.ColumnInt64(7));
                var schematype = checked((int)reader.ColumnInt64(8));
                var schema =  PtrToStringUTF8(reader.ColumnText(9));
                var metadata = PtrToStringUTF8(reader.ColumnText(10));

                return new MetadataRecord()
                {
                    PK = pk,
                    RepoId = repoId,
                    ContainerId = containerId,
                    ParentId = parentId,
                    Counter = counter,
                    Version = version,
                    RemoteId = remoteId,
                    Permissions = permissions,
                    SchemaType = (byte)schematype,
                    Schema = schema == null ? null : JsonSerializer.Deserialize<ContainerSchema>(schema),
                    Metadata = metadata == null ? null : JsonSerializer.Deserialize<Metadata>(metadata)
                };
            }

            private FastQuery CreateFindMetadataQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"SELECT * " +
                          $"from {MetadataTable} " +
                          $"WHERE `PK` LIKE @Pk || '%'";
                // TODO optional params via coalesce
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public List<MetadataRecord> FindMetadata(string path)
            {
                try
                {
                    FindMetadataQuery.Bind((binder, repo) =>
                    {
                        var zData = StringToHGlobalUTF8(repo, out var len);
                        if (binder.BindText(1, zData, len) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind repoPath");
                        }
                    }, path);

                    List<MetadataRecord> result = new List<MetadataRecord>();

                    while (FindMetadataQuery.Step((hasRow, reader, _) =>
                            {
                                if (hasRow)
                                {
                                    var row = MapMetadataTableRow(reader);
                                    result.Add(row);
                                    return true;
                                }
                                return false;
                            }, (object)null)
                    )
                    { }

                    return result;
                }
                finally
                {
                    FindMetadataQuery.Reset();
                }
            }

            private FastQuery CreateGetExactMetadataQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"SELECT * " +
                          $"from {MetadataTable} " +
                          $"WHERE `PK` = @Pk";
                // TODO optional params via coalesce
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public MetadataRecord GetExactMetadata(string repoPath)
            {
                try
                {
                    GetExactMetadataQuery.Bind((binder, repo) =>
                    {
                        var zData = StringToHGlobalUTF8(repo, out var len);
                        if (binder.BindText(1, zData, len) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind repoPath");
                        }
                    }, repoPath);

                    return GetExactMetadataQuery.Step((hasRow, reader, _) =>
                    {
                        if (hasRow)
                        {
                            return MapMetadataTableRow(reader);
                        }
                        return default;
                    }, (object)null);
                }
                finally
                {
                    GetExactMetadataQuery.ClearAndReset();
                }
            }

            private FastQuery CreateIncrementCounterQuery(SqliteConnection connection, ConnectionPool pool)
            {
                var sql = $"UPDATE {MetadataTable} SET `Counter` = `Counter` + 1 " +
                          $"WHERE `PK` = @Pk";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            public int IncrementCounter(string repoPath)
            {
                // TODO exception handling (e.g. SQLite busy)
                // BeginConcurrentQuery.RawStep((hasRow, reader, st) => true, (object)null, out _);
                try
                {
                    IncrementCounterQuery.Bind((binder, repo) =>
                    {
                        var zData = StringToHGlobalUTF8(repo, out var len);
                        if (binder.BindText(1, zData, len) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind repoPath");
                        }
                    }, repoPath);

                    GetExactMetadataQuery.Bind((binder, repo) =>
                    {
                        var zData = StringToHGlobalUTF8(repo, out var len);
                        if (binder.BindText(1, zData, len) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind repoPath");
                        }
                    }, repoPath);

                    if (IncrementCounterQuery.Step((hasRow, reader, _) =>
                    {
                        if (hasRow)
                        {
                            ThrowHelper.FailFast("InsertChunk should not return rows.");
                        }

                        return reader.Changes() == 1;
                    }, (object)null))
                    {
                        return GetExactMetadataQuery.Step((hasRow, reader, _) =>
                        {
                            if (hasRow)
                            {
                                return MapMetadataTableRow(reader).Counter;
                            }
                            return -1;
                        }, (object)null);

                        //// TODO separate query
                        //var md = GetExactMetadata(repoPath, false);
                        //return md.Counter;
                    }
                    else
                    {
                        return -1;
                    }
                }
                finally
                {
                    // CommitQuery.RawStep((hasRow, reader, st) => true, (object)null, out _);
                    IncrementCounterQuery.ClearAndReset();
                    GetExactMetadataQuery.ClearAndReset();
                    //BeginConcurrentQuery.Reset();
                    //CommitQuery.Reset();
                }
            }

            private FastQuery CreateInsertMetadataRowQuery(SqliteConnection connection, ConnectionPool pool)
            {
#pragma warning disable HAA0201 // Implicit string concatenation allocation
                var sql = $"INSERT INTO {MetadataTable} " +
                          $"(`PK`, `RepoId`, `ContainerId`, `ParentId`, `Counter`, `Version`, `RemoteId`, `Permissions`, `SchemaType`, `Schema`, `Metadata`) " +
                          $"VALUES (@PK, @RepoId, @ContainerId, @ParentId, @Counter, @Version, @RemoteId, @Permissions, @SchemaType, @Schema, @Metadata)";
                var fq = new FastQuery(sql, connection, pool);
                return fq;
            }

            // TODO this should return IEnumerable of all new rows that should map to repo creation event
            // For external repos server must send the entire path with ids.
            // This is called inside a txn. A use case is recursive path creation that calls the query multiple times.
            // Parent must exist exactly in this call.

            public bool InsertMetadataRow(MetadataRecord record)
            {
                try
                {
                    InsertMetadataRowQuery.Bind((binder, r) =>
                    {
                        int rc;

                        var zData = StringToHGlobalUTF8(r.PK, out var len);
                        if ((rc = binder.BindText(1, zData, len)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind PK: " + rc);
                        }

                        if ((rc = binder.BindInt64(2, r.RepoId)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind RepoId: " + rc);
                        }

                        if ((rc = binder.BindInt64(3, r.ContainerId)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind ContainerId: " + rc);
                        }

                        if ((rc = binder.BindInt64(4, r.ParentId)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind ParentId: " + rc);
                        }

                        if ((rc = binder.BindInt64(5, r.Counter)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind Counter: " + rc);
                        }

                        if ((rc = binder.BindInt64(6, r.Version)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind Version: " + rc);
                        }

                        if ((rc = binder.BindInt64(7, r.RemoteId)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind RemoteId: " + rc);
                        }

                        if ((rc = binder.BindInt64(8, r.Permissions)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind Permissions: " + rc);
                        }

                        if ((rc = binder.BindInt64(9, r.SchemaType)) != 0)
                        {
                            ThrowHelper.FailFast("Cannot bind SchemaType: " + rc);
                        }

                        if (r.Schema != null)
                        {
                            zData = StringToHGlobalUTF8(JsonSerializer.ToJsonString(r.Schema, ExcludeNullStandardResolver.Instance), out len);
                            if ((rc = binder.BindText(10, zData, len)) != 0)
                            {
                                ThrowHelper.FailFast("Cannot bind Schema: " + rc);
                            }
                        }

                        if (r.Metadata != null)
                        {
                            zData = StringToHGlobalUTF8(JsonSerializer.ToJsonString(r.Metadata, ExcludeNullStandardResolver.Instance), out len);
                            if ((rc = binder.BindText(11, zData, len)) != 0)
                            {
                                ThrowHelper.FailFast("Cannot bind Metadata: " + rc);
                            }
                        }
                    }, record);
                    try
                    {
                        return InsertMetadataRowQuery.Step((hasRow, reader, state) =>
                        {
                            if (hasRow)
                            {
                                ThrowHelper.FailFast("InsertChunk should not return rows");
                            }

                            return reader.Changes() == 1;
                        }, (object)null);
                    }
                    catch
                    {
                        return false;
                    }
                }
                catch (Exception ex)
                {
                    ThrowHelper.FailFast("Cannot insert chunk: " + ex);
                    return false;
                }
                finally
                {
                    InsertMetadataRowQuery.ClearAndReset();
                }
            }
        }
    }
}
