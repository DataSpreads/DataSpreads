# Docs sandbox

Unstructured snippets of documentation drafts that need to bew reviewed, polished and moved to proper sections of documentation.

- [Docs sandbox](#docs-sandbox)
  - [Getting started](#getting-started)
    - [Initialize storage](#initialize-storage)
  - [Process config](#process-config)
  - [DataStore](#datastore)


## Getting started

### Initialize data storage location

By default all data is stored is the default location for application
data. See [`Environment.SpecialFolder.LocalApplicationData`](https://docs.microsoft.com/en-us/dotnet/api/system.environment.specialfolder).

If you have multiple SSD disks and are going to work with 
a lot of data (tens or hundreads of gigabytes) or want to 
reduce load on your system disk (which is a good idea) then 
you could change the default location before you start using
DataSpreads. 

> [!WARNING]
> Currently there is no (easy & documented) way to change 
> the location of existing data storage.

> [!NOTE]
> IF you have a single disk then changing data location 
> within that disk is quite meaningless operation: it will
> not protect from hardware failure and will not improve performance.

To initialize default location call:

```csharp
bool initialized = Data.Storage.Config.InitializeDefaultStore([optional parameters]);
```

See the documentation for this method [here](TODO).

### Initialize storage

First you need to initialize default data storage.

// TODO Actual code for Init()

All parameters are optional and by default data is stored in a user-specific local application data folder (`User/AppData/Local/DataSpreads/data/_default_store` on Windows or `home/.local/share/DataSpreads/data//_default_store` on Linux/MacOS).

Currently you cannot change the location of existing data stores and just moving a folder will make data unavailable from DataSpreads API at least (and maybe corrupted if there are running processes that use a store).

If you have only one disk then do not provide any config parameters and just leave default options. Changes will not have any positive impact on performance.

If you have several SSD disks and do need write ahead logging then choose the fastest and least busy one for WAL. Generally we advise not to use the system disk for WAL, but it depends. If the system disk is a fast NVMe, others are standard SSDs and system activity other then DataSpreads is low then NVMe could still be preferable for WAL even on the system disk. WAL does not require a lot of space but is very sensitive to latency of direct sequential writes.

If you have an HDD with a lot of free space it could be used for optional WAL archiving. The archive process happens in background and does not consume a lot of resources. However, main data storage together with WAL provide durability guarantee for acknowledged writes in case of process/system crash. WAL archive provides additional backup in case of hardware failure but behaves like a tape from which you could restore data slowly and painfully.

You may leave call for `Init()` and it will be noop if parameters are not changed. Otherwise it will throw and you should remove the call after the default data store is initialized.

## Process config

DataSpreads maintains configuration and synchronization files in `AppData/DataSpreads/.config` folder.



## DataStore

DataStore is a physical unit of storage in DataSpreads. It has its own location, synchronizes data streams inside and provides WAL,
compressed storage and archive services.

// TODO This is obviously goes after the concept of data path, repos and prefixes.

There is always a default DataStore present that is used by default and has no data path prefix.
You could create any number of additional data stores at different locations that will have their
name at the beginning of data path prefix. A data store name cannot match any existing data path
in the root data store and no new paths starting with any data store name could exist in 
the default data store.

To create a new data store use a static method [`DataStore.Create`](TDB) or
[`Data.Storage.CreateDataStore`](TBD):

// TODO link to docs, OpenMode, defaults, examples when it makes to separate data in separate stores, e.g. logs.
```csharp
var ds = DataStore.Create("logs", ...)
var ds = Data.Storage.CreateDataStore(...)
```
