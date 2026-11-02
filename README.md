# Dynamic Memory Management for Apache AsterixDB (GSoC 2026)

Alexander Anokhin

Mentor: Dr. Shiva Jahangiri

This repo holds Google Summer of Code 2026 work for Apache AsterixDB: the files
the patch adds or changes, and the patch itself (`memory-broker.patch`).

AsterixDB gives each operator a fixed memory budget at compile time, so memory sits idle while other operators spill. This project adds a per-node broker that moves memory between operators at runtime.

## Main Deliverables
- **ResourceBroker:** a per-node shared data structure that holds a free memory pool, gives each query a fair
  share, and lends idle memory to operators that request memory.
- **Pluggable victimization (`IBrokerStrategy`):** when memory runs out, the broker takes it back from an operator
  above its fair share. Implemented strategies like victimizing largest holder, or victimizing in order of memory difficulty tier.
- **Operator interface:** operators register, request memory, release it when asked, and deregister when done.
- **MemoryContentionResponsiveHHJ:** a memory-adaptive hash join based on Giulliano Siviero's prototype (https://github.com/gSiviero/MemoryContentionHHJ/pull/2), with two
  bug fixes (dropped reused frames, stale buffers after a hash table rebuild).
- **Admission reservation:** each job's memory estimate is reserved on the node before its operators start.

## Status
Not merged yet, Gerrit review pending. Future work is listed at the end of the commit message below.

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
