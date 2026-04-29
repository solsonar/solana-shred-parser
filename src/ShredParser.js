// SPDX-License-Identifier: MIT

import { EventEmitter } from 'node:events';
import { VersionedTransaction } from '@solana/web3.js';

// ─── Solana shred format constants ──────────────────────────────────────────
//
// A Solana shred is a UDP datagram containing a fragment of a slot's entry
// stream. Each datagram has an 88-byte header followed by up to 1115 bytes of
// payload, of which the trailing 88 bytes are an erasure-coding signature and
// must be discarded before reassembly.
//
// Byte layout (little-endian) for the fields we care about:
//   off  64  : variant byte           — bit 0x40 set = coding shred (skip)
//   off  65  : slot                   (u64; we only read the low 32 bits — see note)
//   off  73  : index within slot      (u32)
//   off  85  : flags                  — top two bits 0xC0 = LAST_SHRED_IN_SLOT
//   off  86  : data size              (u16)
//   off  88… : payload (length = data size, last 88 bytes are signature trailer)
//
// Note on slot field: shreds use a u64 slot, but mainnet has not yet exceeded
// 2^32 slots so reading the low 32 bits is safe and faster. If you need 64-bit
// safety, replace with `readBigUInt64LE(65)`.

/** Offset of the shred payload within a UDP datagram. */
export const PAYLOAD_OFFSET = 88;

/** Number of trailing bytes in a shred payload that are signature, not data. */
export const SHRED_SIGNATURE_TRAILER = 88;

/** Maximum number of in-flight slots to keep in memory before evicting oldest. */
export const DEFAULT_MAX_SLOTS = 80;

// ─── Helpers ────────────────────────────────────────────────────────────────

function isDataShred(variantByte) {
  return (variantByte & 0x40) === 0;
}

function isLastInSlot(flagsByte) {
  return (flagsByte & 0xC0) === 0xC0;
}

/**
 * Decode a Solana compact-u16 (ShortVec). Returns `{ val, off }` where `off`
 * is the byte index just past the encoded value.
 *
 * Format: up to 3 bytes; bit 7 of each byte signals continuation.
 *
 * @private
 */
function readCU16(buf, off) {
  let val = 0;
  let shift = 0;
  for (let i = 0; i < 3; i++) {
    if (off >= buf.length) throw new Error('readCU16: out of bounds');
    const b = buf[off++];
    val |= (b & 0x7F) << shift;
    shift += 7;
    if ((b & 0x80) === 0) break;
  }
  return { val, off };
}

/**
 * Compute the byte length of a serialized transaction starting at `start`,
 * without fully deserializing. Used for streaming: we advance through the
 * concatenated entry payload one tx at a time, and need to know whether
 * enough bytes have arrived before attempting `VersionedTransaction.deserialize`.
 *
 * Throws if the buffer is too short or malformed (caller treats as "wait for
 * more bytes" or "give up on this slot").
 *
 * @private
 */
function txByteLength(buf, start) {
  let off = start;
  // signatures
  const ns = readCU16(buf, off); off = ns.off;
  off += ns.val * 64;
  if (off >= buf.length) throw new Error('txByteLength: eof at signatures');
  // version prefix (only present for VersionedTransaction)
  let ver = -1;
  if ((buf[off] & 0x80) !== 0) { ver = buf[off] & 0x7F; off++; }
  // message header (3 bytes)
  if (off + 3 > buf.length) throw new Error('txByteLength: eof at header');
  off += 3;
  // account keys
  const nk = readCU16(buf, off); off = nk.off;
  off += nk.val * 32;
  // recent blockhash (32 bytes)
  off += 32;
  // instructions
  const ni = readCU16(buf, off); off = ni.off;
  for (let i = 0; i < ni.val; i++) {
    if (off >= buf.length) throw new Error(`txByteLength: eof at ix ${i}`);
    off++; // programIdIndex
    const na = readCU16(buf, off); off = na.off; off += na.val;
    const nd = readCU16(buf, off); off = nd.off; off += nd.val;
  }
  // address table lookups (only for v0)
  if (ver === 0) {
    const na2 = readCU16(buf, off); off = na2.off;
    for (let i = 0; i < na2.val; i++) {
      off += 32; // table account
      const nw = readCU16(buf, off); off = nw.off; off += nw.val;
      const nr = readCU16(buf, off); off = nr.off; off += nr.val;
    }
  }
  return off - start;
}

/**
 * Concatenate the contiguous data prefix of a slot's shreds, starting at
 * index 0 and stopping at the first gap. Returns `{ joined, nextIndex }` where
 * `nextIndex` is the first missing shred index.
 *
 * The trailing {@link SHRED_SIGNATURE_TRAILER} bytes of each shred payload
 * are stripped (they are erasure-coding signatures, not data).
 *
 * @private
 */
function joinContiguous(shreds) {
  const parts = [];
  let i = 0;
  while (shreds.has(i)) {
    const { data, dataSize } = shreds.get(i);
    const usable = data.subarray(0, Math.max(0, dataSize - SHRED_SIGNATURE_TRAILER));
    parts.push(usable);
    i++;
  }
  return { joined: Buffer.concat(parts), nextIndex: i };
}

// ─── Types ──────────────────────────────────────────────────────────────────

/**
 * @typedef {object} ShredParserOptions
 * @property {number} [maxSlots]  Maximum in-flight slots before evicting the oldest. Defaults to {@link DEFAULT_MAX_SLOTS}.
 */

/**
 * @typedef {object} TxEvent
 * @property {number} slot           Slot the tx belongs to.
 * @property {import('@solana/web3.js').VersionedTransaction} tx  Deserialized tx.
 * @property {number} shredCount     Number of shreds seen for the slot at the moment of emission.
 * @property {number} firstShredTs   `Date.now()` when the first shred for this slot arrived.
 *                                   Useful for measuring end-to-end shred-to-detect latency.
 */

/**
 * @typedef {object} SlotEvent
 * @property {number} slot
 * @property {number} shredCount
 * @property {number} txCount
 */

/**
 * @typedef {object} ParserStats
 * @property {number} slotsOk      Slots that emitted at least one tx.
 * @property {number} slotsFail    Slots that ended with no parseable txs (usually because we missed the leading shreds).
 * @property {number} txTotal      Total txs counted at slot finalization.
 * @property {number} txStreamed   Total txs emitted via `'tx'` event (deduplicated).
 */

// ─── ShredParser ────────────────────────────────────────────────────────────

/**
 * Stateful assembler that turns raw shreds into `VersionedTransaction`s.
 *
 * Feed every UDP datagram you receive into {@link ShredParser#ingest}. The
 * parser tracks shreds per slot and emits each transaction as a `'tx'` event
 * **as soon as enough contiguous shreds have arrived** — not at slot close.
 * This is what allows downstream consumers to act on a tx before it lands in
 * the confirmed ledger.
 *
 * Slots are evicted in FIFO order once {@link DEFAULT_MAX_SLOTS} slots are
 * in flight, so memory is bounded.
 *
 * @example
 *   import { ShredListener, ShredParser } from 'solana-shred-parser';
 *
 *   const listener = new ShredListener({ port: 20000 });
 *   const parser   = new ShredParser();
 *
 *   listener.on('shred', (msg) => parser.ingest(msg));
 *   parser.on('tx', ({ slot, tx, firstShredTs }) => {
 *     const latency = Date.now() - firstShredTs;
 *     console.log(`slot=${slot} sig=${tx.signatures[0].toString('hex').slice(0,8)} +${latency}ms`);
 *   });
 *
 *   listener.start();
 *
 * @fires ShredParser#slotStart  `({ slot, ts })` — first shred seen for a new slot.
 * @fires ShredParser#tx         `(TxEvent)` — a contiguous tx has been deserialized.
 * @fires ShredParser#slot       `(SlotEvent)` — the last shred of a slot has arrived.
 */
export class ShredParser extends EventEmitter {
  /** @param {ShredParserOptions} [options] */
  constructor(options = {}) {
    super();
    /** @private */
    this._maxSlots = options.maxSlots ?? DEFAULT_MAX_SLOTS;
    /** @private */
    this._slots = new Map();
    /** @private */
    this._stats = { slotsOk: 0, slotsFail: 0, txTotal: 0, txStreamed: 0 };
  }

  /**
   * Feed one UDP datagram into the parser. Non-data shreds and malformed
   * datagrams are silently dropped.
   *
   * @param {Buffer} msg
   */
  ingest(msg) {
    if (msg.length < PAYLOAD_OFFSET || !isDataShred(msg[64])) return;

    const slot     = msg.readUInt32LE(65);
    const index    = msg.readUInt32LE(73);
    const flags    = msg[85];
    const dataSize = msg.readUInt16LE(86);

    let d = this._slots.get(slot);
    if (!d) {
      const firstShredTs = Date.now();
      d = {
        shreds: new Map(),
        done: false,
        firstShredTs,
        nextStreamIndex: 0,    // next contiguous shred index expected
        parseOff: -1,          // byte offset into the joined buffer where the next tx parse starts (-1 = entry header not yet read)
        ne: 0,                 // total entries in slot (read once from header)
        e: 0,                  // entries consumed so far
        remainingTxsInEntry: 0,
        emittedSigs: new Set(),
        slotEmitted: false,
      };
      this._slots.set(slot, d);
      this.emit('slotStart', { slot, ts: firstShredTs });
    }
    if (d.done) return;

    if (dataSize > SHRED_SIGNATURE_TRAILER) {
      d.shreds.set(index, {
        data: msg.slice(PAYLOAD_OFFSET, PAYLOAD_OFFSET + dataSize),
        dataSize,
      });
    }

    this._streamParse(slot, d);

    if (isLastInSlot(flags)) {
      d.done = true;
      this._finalizeSlot(slot, d);
      return;
    }

    if (this._slots.size > this._maxSlots) {
      const oldest = [...this._slots.keys()].sort((a, b) => a - b)[0];
      this._slots.delete(oldest);
    }
  }

  /**
   * Try to consume any contiguous bytes added since the last call and emit
   * each newly-decodable tx via the `'tx'` event.
   *
   * @private
   */
  _streamParse(slot, d) {
    if (!d.shreds.has(d.nextStreamIndex)) return;

    const { joined, nextIndex } = joinContiguous(d.shreds);
    d.nextStreamIndex = nextIndex;

    // Read entry header once: 8 bytes = total entry count
    if (d.parseOff < 0) {
      if (joined.length < 8) return;
      d.ne = Number(joined.readBigUInt64LE(0));
      if (d.ne === 0 || d.ne > 10000) {
        // Bad header — disable streaming for this slot.
        d.parseOff = Number.MAX_SAFE_INTEGER;
        return;
      }
      d.parseOff = 8;
    }

    let off = d.parseOff;

    while (d.e < d.ne) {
      // Entry boundary: read 40-byte entry header (8 hash_count + 32 hash + 8 num_txs)
      if (d.remainingTxsInEntry === 0) {
        if (off + 48 > joined.length) break;
        off += 8 + 32; // skip hash_count + hash
        const numTxs = Number(joined.readBigUInt64LE(off)); off += 8;
        if (numTxs > 100000) {
          d.parseOff = Number.MAX_SAFE_INTEGER;
          return;
        }
        d.remainingTxsInEntry = numTxs;
        if (numTxs === 0) {
          d.e++;
          continue;
        }
      }

      // Probe length without committing to a deserialize
      let size;
      try {
        size = txByteLength(joined, off);
      } catch {
        // Not enough bytes yet — wait for more shreds.
        break;
      }
      if (size <= 0 || size > 1500) {
        d.parseOff = Number.MAX_SAFE_INTEGER;
        return;
      }
      if (off + size > joined.length) break;

      let tx;
      try {
        tx = VersionedTransaction.deserialize(joined.slice(off, off + size));
      } catch {
        d.parseOff = Number.MAX_SAFE_INTEGER;
        return;
      }
      off += size;
      d.remainingTxsInEntry--;
      if (d.remainingTxsInEntry === 0) d.e++;

      // Dedup by first signature (turbine occasionally re-broadcasts shreds).
      const sig0 = tx.signatures[0];
      if (sig0) {
        const sigKey = Buffer.from(sig0).toString('hex');
        if (d.emittedSigs.has(sigKey)) continue;
        d.emittedSigs.add(sigKey);
      }

      this._stats.txStreamed++;
      this.emit('tx', {
        slot,
        tx,
        shredCount: d.shreds.size,
        firstShredTs: d.firstShredTs,
      });
    }

    d.parseOff = off;
  }

  /** @private */
  _finalizeSlot(slot, d) {
    // One last streaming pass — late shreds may have filled gaps.
    this._streamParse(slot, d);

    if (d.emittedSigs.size === 0) {
      this._stats.slotsFail++;
      return;
    }
    this._stats.slotsOk++;
    this._stats.txTotal += d.emittedSigs.size;
    if (!d.slotEmitted) {
      d.slotEmitted = true;
      this.emit('slot', {
        slot,
        shredCount: d.shreds.size,
        txCount: d.emittedSigs.size,
      });
    }
  }

  /**
   * Number of shreds currently buffered for a given slot (0 if unknown).
   * @param {number} slot
   * @returns {number}
   */
  shredCount(slot) {
    return this._slots.get(slot)?.shreds.size ?? 0;
  }

  /** @returns {ParserStats} A snapshot of cumulative counters. */
  stats() {
    return { ...this._stats };
  }
}
