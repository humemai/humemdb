# Vectors

The current vector surface is deliberately conservative.

## Storage model

- vectors are stored in SQLite as float32 blobs
- one collection is loaded as one NumPy matrix
- dimensions must be consistent within a collection

## Search model

- exact search only on the public path
- optional bucket filtering before scoring
- metrics: `cosine`, `dot`, `l2`

## Why exact search is the default

HumemDB needs a defensible baseline before it broadens into indexed ANN routing. The
exact SQLite plus NumPy path is simple enough to benchmark honestly and strong enough to
set the first public contract.

## Relationship to LanceDB

LanceDB is in the dependency set because benchmark and future accelerated work already
exists around that direction. It is not the default public runtime path today.