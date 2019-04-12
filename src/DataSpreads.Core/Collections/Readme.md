# Persistent Collections

## TODO:

* DataChannel<TOut,TIn> - duplex data stream
* PersistentMap - KV LSM-like approach with in-memory head batch-merged with normal LMDB tail.
* PersistentSeries - Spreads's Series (Append/Mutable or same via a flag).
* PersistentMatrix
* PersistentFrame - Multi-column series. Columns are fixed and are part of schema.
* PersistentPanel - Series of dynamic membership + optional weights/count (double/decimal or generic?).