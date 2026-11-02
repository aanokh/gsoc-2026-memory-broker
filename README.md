# Dynamic Memory Management for Apache AsterixDB (GSoC 2026)

This repo holds my Google Summer of Code 2026 work for Apache AsterixDB: the files
the patch adds or changes, and the patch itself (`memory-broker.patch`).

## How to apply and build

The patch applies on top of AsterixDB master at commit `f6950e7bfe`:

    git clone https://github.com/apache/asterixdb.git
    cd asterixdb
    git checkout f6950e7bfe
    git am /path/to/memory-broker.patch
    mvn clean install -DskipTests

## Commit message

    [ASTERIXDB-3703][HYR] Add a per-node memory broker and a memory adaptive hash join

    - user model changes: no
    - storage format changes: no
    - interface changes: yes

    Details: Adds a ResourceBroker on each NC that distributes operator
    memory in frames during runtime. Victim selection when memory runs out
    is pluggable through IBrokerStrategy. Before a job's operators start,
    the broker reserves the job's existing compile-time memory estimate for
    that node, passed per node as requiredNodeMemory in JobSpecification.

    APE link: https://cwiki.apache.org/confluence/spaces/ASTERIXDB/pages/449282523/APE+35+Dynamic+Operator+Memory+Broker

    MemoryContentionResponsiveHHJ requests memory when a partition fills and
    spills when its budget is cut. It is based on Giulliano Siviero's
    prototype: https://github.com/gSiviero/MemoryContentionHHJ/pull/2
    Fixed-budget operators register their budget through new static frame
    pools.

    On top of the prototype, VPartitionTupleBufferManager now restamps the
    frame size header when it reuses a pool frame, which previously dropped
    frames, and InMemoryHashJoin clears stale buffers and recomputes its
    empty-table flag when the hash table is rebuilt during the probe.

    Future work: consider fair shares per query, explore how memory tiering
    interacts with other operators, and explore more advanced victimization
    strategies.
