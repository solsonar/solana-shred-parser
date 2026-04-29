# solana-shred-parser

Low-latency Solana shred listener and assembler. Receives raw shreds over UDP and reconstructs `VersionedTransaction` objects in real time — typically tens to hundreds of milliseconds before the same transactions reach the confirmed RPC ledger.

```js
import { ShredListener, ShredParser } from 'solana-shred-parser';

const listener = new ShredListener({ port: 20000 });
const parser   = new ShredParser();

listener.on('shred', (msg) => parser.ingest(msg));
parser.on('tx', ({ slot, tx, firstShredTs }) => {
  const sig = Buffer.from(tx.signatures[0]).toString('hex').slice(0, 16);
  console.log(`slot=${slot} sig=${sig} +${Date.now() - firstShredTs}ms`);
});

listener.start();
```

## What this package does

- Binds a UDP socket on the port you choose
- Sets a large kernel receive buffer (default 128 MB) to survive shred bursts
- Reassembles each slot's data shreds into the contiguous entry stream
- Walks the entry stream and emits each `VersionedTransaction` **as soon as enough contiguous shreds have arrived** — not at slot close

Two classes, no dependencies beyond `@solana/web3.js` (peer):

- `ShredListener` — UDP socket, emits raw shred datagrams
- `ShredParser` — stateful assembler, emits `VersionedTransaction`s

## What this package does not do

- It does not connect to a validator or shred relay for you. You are responsible for arranging shred traffic to reach your UDP port (typically Jito ShredStream, a self-hosted validator forwarding turbine, or a relay your provider runs).
- It does not interpret transactions. There is no DEX awareness, no instruction decoding, no account filtering. You consume the `VersionedTransaction` and apply your own logic.
- It does not handle erasure-coded recovery from coding shreds. Coding shreds are filtered out — the parser only operates on data shreds. In practice this is rarely a limitation: most slots ship with full data shred coverage.

If you need transaction-level filtering, instruction decoding, address-lookup-table resolution, or DEX classification, build it on top.

## Install

```sh
npm install solana-shred-parser @solana/web3.js
```

`@solana/web3.js@^1.91.0` is a peer dependency — install it in your project.

Requires Node 18+ (uses native `fetch`-era APIs and `BigInt`).

## How shreds work (background)

A Solana validator's TPU produces a stream of *entries*, each containing zero or more transactions. Entries are batched into *slots* (one slot ≈ 400 ms). To distribute slot data across the cluster, the leader splits each slot into a sequence of *shreds* — fixed-size UDP datagrams — and broadcasts them via the turbine tree.

Each shred carries:
- A header identifying its slot, index within the slot, and a `LAST_IN_SLOT` flag
- A payload (up to ~1 KB) — a fragment of the entry stream, plus a signature trailer

To recover a transaction, you need to:
1. Buffer shreds by `(slot, index)`
2. Concatenate the contiguous prefix from index 0
3. Strip each shred's signature trailer (last 88 bytes of the payload)
4. Walk the resulting byte stream as a sequence of entries and transactions

This package does steps 1–4 and emits each transaction the moment it becomes whole.

## API

### `new ShredListener(options)`

| option         | type    | default     | notes                                                           |
| -------------- | ------- | ----------- | --------------------------------------------------------------- |
| `port`         | number  | (required)  | UDP port to bind                                                |
| `host`         | string  | `'0.0.0.0'` | Interface to bind to                                            |
| `recvBufferMb` | number  | `128`       | Kernel receive buffer size in MB                                |

The kernel may cap `recvBufferMb` below the requested value. On Linux, raise the ceiling with `sysctl -w net.core.rmem_max=<bytes>`.

#### Methods

- `start()` — bind the socket. Throws if called twice.
- `stop()` — close the socket. Idempotent.
- `stats()` — `{ packets, bytes }` snapshot.

#### Events

- `'listening'` — `{ port, host, recvBufferMb }` — socket bound and configured.
- `'shred'` — `(msg: Buffer)` — one raw datagram.
- `'error'` — `(err: Error)`.
- `'close'` — socket closed.

### `new ShredParser(options?)`

| option     | type   | default | notes                                                  |
| ---------- | ------ | ------- | ------------------------------------------------------ |
| `maxSlots` | number | `80`    | In-flight slots to keep before evicting the oldest      |

#### Methods

- `ingest(msg: Buffer)` — feed one datagram. Non-data shreds and malformed datagrams are dropped silently.
- `shredCount(slot: number)` — number of shreds buffered for a given slot.
- `stats()` — `{ slotsOk, slotsFail, txTotal, txStreamed }`.

#### Events

- `'slotStart'` — `{ slot, ts }` — first shred for a new slot. `ts` is `Date.now()` and is the reference for the `firstShredTs` field on `'tx'` events.
- `'tx'` — `{ slot, tx, shredCount, firstShredTs }` — a transaction has been deserialized.
- `'slot'` — `{ slot, shredCount, txCount }` — the last shred of a slot has arrived.

`tx` is a `VersionedTransaction` — its `message.compiledInstructions`, `message.staticAccountKeys`, and `message.addressTableLookups` are populated. To resolve account keys behind ALTs you need to fetch the lookup tables yourself (out of scope for this package).

### Constants

```js
import {
  DEFAULT_RECV_BUFFER_MB,    // 128
  PAYLOAD_OFFSET,            // 88
  SHRED_SIGNATURE_TRAILER,   // 88
  DEFAULT_MAX_SLOTS,         // 80
} from 'solana-shred-parser';
```

## Performance

On a Linux server colocated with a Jito ShredStream relay, a single Node 20 process running the example pipeline reliably handles full mainnet shred volume:

| metric                          | value                |
| ------------------------------- | -------------------- |
| UDP packets / second            | 50–100k (peak ~150k) |
| Transactions emitted / second   | 5–10k                |
| Median shred → tx latency       | 20–50 ms             |
| 95p shred → tx latency          | 100–200 ms           |
| Memory (RSS, steady state)      | ~250 MB              |

Numbers depend heavily on your shred source and network locality.

## Examples

See [`examples/`](./examples) for runnable scripts:

- `basic-listener.js` — minimal pipeline
- `filter-by-program.js` — emit only txs that touch a given program ID
- `stats.js` — drop-rate and throughput counters

Run with `node examples/basic-listener.js` (after pointing some shred traffic at port 20000).

## Troubleshooting

**No `'shred'` events:** verify shred traffic is actually arriving with `tcpdump -i any -n udp port 20000`. Most shred relays require IP allowlisting on their side.

**`'shred'` events but no `'tx'` events:** typically you are missing leading shreds (index 0…N). The parser cannot start a slot's entry stream without index 0. This is fine — subsequent slots will start cleanly.

**Receive buffer smaller than requested:** the OS capped it. On Linux: `sudo sysctl -w net.core.rmem_max=$((256*1024*1024))`. Re-create the listener afterward.

**`'tx'` events stop after a while:** check `stats()` — if `slotsFail` keeps climbing it usually means shreds are being dropped at the kernel level (buffer too small for your bursts).

## License

MIT
