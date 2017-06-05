# Quebic

**Quebic** is a simple and small local journaling queue library. It is intended to be used in pipeline data processing application especially machine learning or ETL.

The Quebic supports $n$:$1$ operation on multi-thread or multi-process environment. Note that the only one subscriber can receive the pushed data.

Once a data is pushed into queue, it will be able to read at the next time even if the process goes down. In addition, the last pushed data remains in queue and it can refer anytime even if the queue is empty. This allows your application's process to determine where it should resume from.

This library is based on double-stacking queue with only two files.

## Features

* Persistent queue that can stop and restart processing.
* Process large amounts of data that exceed JavaVM heap.
* Remember the latest push data to determine restart position.
* Compression option for efficient storing large JSON or vector.
* Data type verification with simple schema.
* Simple two-files data store.
* $n$:$1$ parallel push, parallel pop.

### Performance

| Operation | 
|:----------|
| PUSH      |
| POP       |