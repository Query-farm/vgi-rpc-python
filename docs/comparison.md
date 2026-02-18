---
hide:
  - navigation
---

# Comparison with Other Frameworks

An honest comparison of Python RPC frameworks. Every framework makes trade-offs — this page helps you pick the right one for your use case.

## At a Glance

| Framework | Serialization | IDL Required | Streaming | Typed Proxies | Transports | Maintained |
|-----------|--------------|--------------|-----------|---------------|------------|------------|
| **vgi-rpc** | Arrow IPC | No (Protocol classes) | Producer + Exchange | Yes (mypy strict) | Pipe, subprocess, shm, HTTP | Yes |
| **gRPC** | Protobuf | Yes (`.proto`) | All 4 patterns | Partial (needs mypy-protobuf) | HTTP/2 | Yes (Google) |
| **Apache Thrift** | Thrift binary/compact | Yes (`.thrift`) | No | No | TCP, HTTP | Moderate |
| **ConnectRPC** | Protobuf | Yes (`.proto`) | All 4 patterns | Yes | HTTP/1.1 + HTTP/2 | Yes (beta) |
| **Cap'n Proto** | Cap'n Proto | Yes (`.capnp`) | Promise pipelining | No | TCP (asyncio) | Yes |
| **RPyC** | Brine (custom) | No | No | No | TCP | Low |
| **Pyro5** | Serpent / JSON / msgpack | No | No | No | TCP | Low |
| **ZeroRPC** | MessagePack | No | Server only | No | ZeroMQ (TCP/IPC) | Moderate |
| **xmlrpc** | XML | No | No | No | HTTP | stdlib |
| **JSON-RPC** | JSON | No | No | No | HTTP | Varies |

## Detailed Comparison

### gRPC

The industry standard. If you need cross-language interop with Java, Go, C++, or a large ecosystem of middleware, gRPC is the safe choice.

**Strengths:**

- Full streaming support (unary, server, client, bidirectional)
- Massive ecosystem — load balancing, service mesh integration, observability
- HTTP/2 multiplexing for concurrent RPCs over one connection
- Backed by Google, six-week release cadence
- Async support via `grpc.aio`
- ~44k GitHub stars

**Trade-offs:**

- Requires `.proto` files and a `protoc` compile step — adds friction for Python-only projects
- Generated Python code is not idiomatic; type stubs need [mypy-protobuf](https://github.com/nipunn1313/mypy-protobuf) as an extra step
- HTTP/2 only — no pipe, subprocess, or shared memory transports
- Protobuf is row-oriented — inefficient for columnar/tabular data workloads
- Heavy C++ dependency chain

**Best for:** Polyglot microservices, production APIs needing ecosystem tooling, teams already using protobuf.

### Apache Thrift

Facebook's original RPC framework. Still used in legacy systems but losing ground to gRPC.

**Strengths:**

- Cross-language (C++, Java, Python, PHP, Ruby, Erlang, and more)
- Pluggable transports and protocols (binary, compact, JSON, framed, buffered)
- Mature and battle-tested (since ~2007)

**Trade-offs:**

- No streaming support — this is a fundamental architectural limitation
- Declining community; many projects have migrated to gRPC
- Requires `.thrift` IDL files and code generation
- Weak Python typing/IDE story
- ~11k GitHub stars, ~2 releases per year

**Best for:** Legacy systems already using Thrift, polyglot environments where Thrift is the org standard.

### ConnectRPC

A modern, cleaner take on gRPC. Compatible with the gRPC wire protocol but works over HTTP/1.1 and generates better Python code.

**Strengths:**

- gRPC-compatible without gRPC's complexity
- Works over HTTP/1.1 (no HTTP/2 requirement for unary and server streaming)
- Clean generated code with proper type annotations
- Supports Connect, gRPC, and gRPC-Web protocols
- Backed by [Buf](https://buf.build/)

**Trade-offs:**

- Very new and still in beta (v0.5.0, ~49 GitHub stars)
- Requires Python 3.13+ (narrow adoption window)
- Still requires `.proto` files and code generation
- Not yet battle-tested in production
- API may have breaking changes before 1.0

**Best for:** New projects that want gRPC compatibility with a better Python DX, if you can accept the beta risk.

### Cap'n Proto (pycapnp)

Zero-copy serialization with capability-based security. Unique promise pipelining reduces round trips.

**Strengths:**

- Extremely fast serialization (zero-copy design — data read directly from the wire)
- Promise pipelining: use the return of one RPC as the argument to the next without waiting
- Runtime schema loading (no compile step, unlike protobuf)
- Actively maintained (v2.2.2, Jan 2026)

**Trade-offs:**

- Requires `.capnp` schema files
- Dynamic Python API — no IDE autocompletion for remote methods
- Small Python community (~473 GitHub stars)
- C++ dependency (Cython bindings)
- Sparse documentation
- Mandatory asyncio (all RPC calls require an event loop)

**Best for:** Performance-critical systems where zero-copy serialization matters, projects that benefit from promise pipelining.

### RPyC

Transparent remote object proxying — call remote methods as if they were local, with zero boilerplate.

**Strengths:**

- Truly transparent: no IDL, no decorators, no special definitions
- Symmetric — both ends can call each other
- Zero friction for prototyping

**Trade-offs:**

- No type safety or IDE support at all
- Security concerns (object proxying exposes remote internals)
- Not designed for high-throughput data transfer
- Python-only
- Low maintenance activity (~1.7k GitHub stars)

**Best for:** Quick prototyping, internal tools, Python-to-Python scripting where type safety doesn't matter.

### Pyro5

Python Remote Objects — expose Python objects over the network with a name server for discovery.

**Strengths:**

- 100% pure Python, no C dependencies
- Built-in name server for service discovery
- Multiple serializer options (serpent, JSON, msgpack)
- Simple `@expose` decorator API

**Trade-offs:**

- No type safety or IDE support for remote calls
- No protocol-level streaming
- Python-only
- Small community (~347 GitHub stars), low maintenance
- Not suited for high-throughput workloads

**Best for:** Simple Python-to-Python distributed computing, service discovery use cases.

### ZeroRPC

MessagePack serialization over ZeroMQ. Includes a CLI tool and server streaming.

**Strengths:**

- Simple API — expose any Python object with one line
- ZeroMQ provides heartbeats, timeouts, and reliable messaging
- Server streaming support
- Compact MessagePack serialization
- CLI tool for command-line RPC

**Trade-offs:**

- Hard dependency on gevent (incompatible with asyncio)
- No bidirectional streaming
- No type safety
- Python-only (Node.js implementation exists separately)

**Best for:** Simple services where gevent is acceptable and you want built-in streaming with minimal setup.

### xmlrpc / JSON-RPC

The simplest option. xmlrpc is in the Python standard library; JSON-RPC libraries are lightweight.

**Strengths:**

- xmlrpc: zero dependencies (stdlib), works everywhere
- JSON-RPC: human-readable, easy to debug
- Lowest barrier to entry
- Wide interoperability (any language with HTTP)

**Trade-offs:**

- No streaming
- Verbose, inefficient serialization
- No type safety
- xmlrpc has security concerns; binary data requires base64 encoding
- Poor performance for large data volumes

**Best for:** Quick scripts, simple internal tools, interop with systems that only speak XML-RPC or JSON-RPC.

## When to Use What

| Use case | Recommended |
|----------|-------------|
| Polyglot microservices (Java, Go, Python) | gRPC |
| Python-only, typed interfaces, no code generation | vgi-rpc |
| Columnar / tabular data transfer | vgi-rpc |
| Co-located processes, zero-copy shared memory | vgi-rpc |
| Quick prototype, zero boilerplate | RPyC or ZeroRPC |
| Simplest possible, stdlib only | xmlrpc |
| gRPC compatibility with better Python DX | ConnectRPC |
| Legacy Thrift systems | Apache Thrift |
| Zero-copy serialization, promise pipelining | Cap'n Proto |

## Feature Deep Dives

### Serialization Format

The serialization format determines both performance characteristics and what kinds of data transfer efficiently:

- **Row-oriented** (protobuf, msgpack, JSON, XML, Thrift): Each message is a set of key-value fields. Efficient for small, heterogeneous messages (like API requests). Inefficient for large tabular datasets.
- **Columnar** (Arrow IPC): Data is organized by column. Efficient for analytical workloads, DataFrames, and bulk data transfer. A 10K-row batch with three columns serializes as three contiguous arrays — no per-row overhead.
- **Zero-copy** (Cap'n Proto, Arrow IPC via shared memory): Data can be read directly without parsing. Cap'n Proto achieves this at the message level; vgi-rpc achieves this for Arrow batches via `ShmPipeTransport`.

### Interface Definition

Frameworks fall into two camps:

**External IDL** (gRPC, Thrift, ConnectRPC, Cap'n Proto): Define interfaces in a separate schema language, then generate Python code. Pros: language-agnostic contracts, strong backwards-compatibility guarantees. Cons: extra tooling, generated code may not be idiomatic Python.

**Python-native** (vgi-rpc, RPyC, Pyro5, ZeroRPC, xmlrpc): Define interfaces directly in Python. Pros: no code generation, natural Python experience. Cons: Python-only contracts (though vgi-rpc's Arrow schemas are language-neutral).

vgi-rpc is unique in this space: it uses Python `Protocol` classes (standard typing) as the IDL, but derives language-neutral Arrow schemas from the type annotations. You get the Python-native experience with a wire format that other Arrow-aware languages can consume.

### Type Safety

| Level | Frameworks |
|-------|-----------|
| **mypy strict + IDE autocompletion** | vgi-rpc, ConnectRPC (generated) |
| **Generated stubs (extra tooling)** | gRPC (with mypy-protobuf) |
| **Runtime types only** | Thrift, Cap'n Proto |
| **None** | RPyC, Pyro5, ZeroRPC, xmlrpc, JSON-RPC |

### Streaming Patterns

| Pattern | gRPC | ConnectRPC | vgi-rpc | ZeroRPC | Others |
|---------|------|------------|---------|---------|--------|
| Unary | Yes | Yes | Yes | Yes | Yes |
| Server streaming | Yes | Yes | Yes (producer) | Yes | No |
| Client streaming | Yes | Yes | No | No | No |
| Bidirectional | Yes | Half-duplex | Yes (exchange) | No | No |

vgi-rpc's exchange pattern is lockstep (one request, one response, repeat) rather than fully concurrent bidirectional. This is simpler to reason about and sufficient for most streaming use cases, but not equivalent to gRPC's full-duplex bidirectional streams.
