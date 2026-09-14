# InElasticsearch

A small clone of Elasticsearch, built on top of the [Apache Lucene](https://lucene.apache.org/) API. Lucene handles indexing and search on a single node; this project wraps it with a distributed system layer — sharding, replication, and routing — to turn a handful of single-node Lucene instances into one cluster.

Unlike Elasticsearch, clients and inter-node communication both go over [gRPC](https://grpc.io/) instead of HTTP/REST. That trades away curl-friendly debugging for lower latency and strongly-typed contracts, which felt like the more interesting problem to solve.

## Architecture

- **Server** — one process per data node. Owns a set of shards (primaries and/or replicas), backed by a local Lucene index, and serves reads/writes for them over gRPC.
- **Coordinator** — the single cluster-aware front door. Speaks the exact same gRPC contract as a `Server`, so to a client it's indistinguishable from talking to one giant node. Behind that facade it computes shard routing, load-balances reads across a shard's primary and replicas, retries against a replica when a copy doesn't answer, and fans out writes to replicas after a primary applies them.
- **ClusterTopology** — a small config-driven model (`cluster.conf`) shared by every process, mapping shard IDs to owning nodes so each one independently computes identical routing.
- **Client** — a runnable demo that indexes sample documents and runs a few classic Lucene query shapes (term, exact keyword, phrase, fuzzy, boolean) against a running cluster.
- **LoadGenerator** — a manual stress-test tool that drives concurrent index/search/delete traffic against a live cluster and reports throughput, latency, and error rates, useful for exercising failover and routing under real concurrency.

All of these speak one gRPC contract, `DataNodeService` (defined in `src/main/proto/inelasticsearch.proto`), split into a client-facing surface (`IndexDocument`, `BulkIndexDocument`, `DeleteDocument`, `Search`) and peer-to-peer-only replication RPCs (`ReplicateDocument`, `ReplicateBulkIndex`, `ReplicateDelete`) used by a shard's primary to push writes to its replicas.

## Building

Requires Java and Maven.

```
mvn compile
```

This also regenerates the gRPC/protobuf Java sources from `inelasticsearch.proto` via the `protobuf-maven-plugin`.

## Running a cluster

`cluster.conf` (bundled under `src/main/resources`) defines the default 3-node topology:

```
totalShards=6
replicas=1
node-a=localhost:9091
node-b=localhost:9092
node-c=localhost:9093
```

Start one `Server` per node listed in the config, then the `Coordinator` in front of them, each in its own terminal:

```
mvn exec:java -Dexec.mainClass=com.example.inelasticsearch.Server -Dexec.args="node-a"
mvn exec:java -Dexec.mainClass=com.example.inelasticsearch.Server -Dexec.args="node-b"
mvn exec:java -Dexec.mainClass=com.example.inelasticsearch.Server -Dexec.args="node-c"
mvn exec:java -Dexec.mainClass=com.example.inelasticsearch.Coordinator
```

Storage is a fresh temp directory per run — there's no persistence across restarts.

Then, against the running coordinator:

```
mvn exec:java -Dexec.mainClass=com.example.inelasticsearch.Client
```

`Client` takes optional `[host] [port]` arguments (default `localhost 7000`) — point it at a lone `Server` instead of the `Coordinator` to bypass sharding entirely.

To stress-test the cluster:

```
mvn exec:java -Dexec.mainClass=com.example.inelasticsearch.LoadGenerator -Dexec.args="localhost 7000 30 8"
```

Arguments are `[host] [port] [durationSeconds] [concurrency]`. It's a good way to watch replica fallback and shard routing behave under load — kill a `Server` process mid-run in another terminal and see the error rate react and recover.

## Testing

```
mvn test
```

Covers cluster topology and shard routing, index/search behavior, replication, and coordinator failover.

## Project layout

```
src/main/proto/inelasticsearch.proto        gRPC contract shared by every process
src/main/java/.../cluster/                  shard routing, topology, node addressing
src/main/java/.../coordinator/              coordinator-side routing, replica selection
src/main/java/.../grpc/                     gRPC service impls, wire <-> Lucene document mapping
src/main/java/.../index/                    Lucene-backed indexing and search
src/main/java/.../{Server,Coordinator,Client,LoadGenerator}.java   process entry points
```
