// Smoke tests for the parser. These do not exercise the full reassembly
// pipeline (which requires real shred bytes from a live cluster) — they verify
// that the public surface behaves as documented.
//
// Run with: node --test test/

import { test } from 'node:test';
import { strict as assert } from 'node:assert';
import {
  ShredParser,
  ShredListener,
  PAYLOAD_OFFSET,
  SHRED_SIGNATURE_TRAILER,
  DEFAULT_MAX_SLOTS,
  DEFAULT_RECV_BUFFER_MB,
} from '../src/index.js';

test('exports public constants', () => {
  assert.equal(PAYLOAD_OFFSET, 88);
  assert.equal(SHRED_SIGNATURE_TRAILER, 88);
  assert.equal(DEFAULT_MAX_SLOTS, 80);
  assert.equal(DEFAULT_RECV_BUFFER_MB, 128);
});

test('ShredParser ignores datagrams smaller than the header', () => {
  const parser = new ShredParser();
  let txEmitted = 0;
  parser.on('tx', () => txEmitted++);
  parser.ingest(Buffer.alloc(50));
  parser.ingest(Buffer.alloc(0));
  assert.equal(txEmitted, 0);
  assert.deepEqual(parser.stats(), { slotsOk: 0, slotsFail: 0, txTotal: 0, txStreamed: 0 });
});

test('ShredParser ignores coding shreds (variant byte bit 0x40 set)', () => {
  const parser = new ShredParser();
  const buf = Buffer.alloc(200);
  buf[64] = 0x40; // coding shred marker
  buf.writeUInt32LE(123, 65);  // slot
  buf.writeUInt32LE(0, 73);    // index
  buf[85] = 0;                 // flags
  buf.writeUInt16LE(100, 86);  // dataSize

  let slotStarts = 0;
  parser.on('slotStart', () => slotStarts++);
  parser.ingest(buf);
  assert.equal(slotStarts, 0, 'coding shreds should not register as a new slot');
});

test('ShredParser registers a data shred and emits slotStart', () => {
  const parser = new ShredParser();
  const buf = Buffer.alloc(200);
  buf[64] = 0x00; // data shred
  buf.writeUInt32LE(456, 65);
  buf.writeUInt32LE(0, 73);
  buf[85] = 0;
  buf.writeUInt16LE(100, 86);

  let started = null;
  parser.on('slotStart', (e) => { started = e; });
  parser.ingest(buf);
  assert.equal(started?.slot, 456);
  assert.equal(typeof started?.ts, 'number');
  assert.equal(parser.shredCount(456), 1);
});

test('ShredParser caps in-flight slots at maxSlots', () => {
  const parser = new ShredParser({ maxSlots: 4 });
  const make = (slot) => {
    const buf = Buffer.alloc(200);
    buf[64] = 0;
    buf.writeUInt32LE(slot, 65);
    buf.writeUInt32LE(0, 73);
    buf[85] = 0;
    buf.writeUInt16LE(100, 86);
    return buf;
  };
  for (let s = 1; s <= 10; s++) parser.ingest(make(s));
  // After ingestion, only the most recent ~4 slots should remain.
  assert.ok(parser.shredCount(1) === 0, 'oldest slot should be evicted');
  assert.ok(parser.shredCount(10) >= 1, 'newest slot should remain');
});

test('ShredListener throws when port is missing', () => {
  assert.throws(() => new ShredListener({}), /port/);
  assert.throws(() => new ShredListener(), /port/);
});

test('ShredListener throws when start() is called twice', () => {
  const listener = new ShredListener({ port: 0, recvBufferMb: 1 });
  // Don't actually bind — just call start() to flip the started flag.
  // The dgram.bind() call below kicks off async work but we reject the second
  // start() synchronously via the `_started` guard.
  listener.start();
  assert.throws(() => listener.start(), /already/);
  listener.stop();
});

test('ShredListener.start() binds and emits listening event', { timeout: 5000 }, async (t) => {
  const listener = new ShredListener({ port: 0, recvBufferMb: 1 });
  t.after(() => listener.stop());
  const event = await new Promise((resolve, reject) => {
    listener.once('listening', resolve);
    listener.once('error', reject);
    listener.start();
  });
  assert.equal(typeof event.port, 'number');
  assert.equal(event.host, '0.0.0.0');
});
