// Print throughput + drop-rate statistics every 5 seconds.
//
//   node examples/stats.js [port]
//
// Useful for sizing your kernel receive buffer and verifying that your shred
// source is healthy.

import { ShredListener, ShredParser } from '../src/index.js';

const port = Number(process.argv[2] ?? 20000);

const listener = new ShredListener({ port });
const parser   = new ShredParser();

listener.on('shred', (msg) => parser.ingest(msg));
listener.on('error', (err) => console.error('[listener]', err));
listener.start();

let prev = { packets: 0, bytes: 0, txStreamed: 0, slotsOk: 0, slotsFail: 0 };
const startedAt = Date.now();

setInterval(() => {
  const l = listener.stats();
  const p = parser.stats();
  const elapsedSec = (Date.now() - startedAt) / 1000;

  const dPackets = l.packets - prev.packets;
  const dBytes   = l.bytes   - prev.bytes;
  const dTx      = p.txStreamed - prev.txStreamed;
  const dOk      = p.slotsOk    - prev.slotsOk;
  const dFail    = p.slotsFail  - prev.slotsFail;

  prev = { packets: l.packets, bytes: l.bytes, txStreamed: p.txStreamed, slotsOk: p.slotsOk, slotsFail: p.slotsFail };

  const failRate = (dOk + dFail) > 0 ? (dFail / (dOk + dFail) * 100).toFixed(1) : '0.0';
  const mbPerSec = (dBytes / 5 / 1024 / 1024).toFixed(2);

  console.log(
    `t=${elapsedSec.toFixed(0)}s | ` +
    `pkt/s=${(dPackets/5).toFixed(0)} (${mbPerSec} MB/s) | ` +
    `tx/s=${(dTx/5).toFixed(0)} | ` +
    `slots ok=${dOk} fail=${dFail} (${failRate}%)`,
  );
}, 5_000);
