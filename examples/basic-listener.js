// Minimal end-to-end pipeline: UDP shreds in, VersionedTransactions out.
//
//   node examples/basic-listener.js [port]
//
// Default port is 20000. Point your shred source (e.g. Jito ShredStream relay
// or a forwarded turbine feed) at the chosen port before running.

import { ShredListener, ShredParser } from '../src/index.js';

const port = Number(process.argv[2] ?? 20000);

const listener = new ShredListener({ port });
const parser   = new ShredParser();

listener.on('listening', ({ port, recvBufferMb }) => {
  console.log(`[listener] bound to :${port}, recv buffer ${recvBufferMb} MB`);
});
listener.on('shred', (msg) => parser.ingest(msg));
listener.on('error', (err) => console.error('[listener]', err));

parser.on('tx', ({ slot, tx, firstShredTs }) => {
  const sig = Buffer.from(tx.signatures[0]).toString('hex').slice(0, 16);
  const numIxs = tx.message.compiledInstructions.length;
  const numKeys = tx.message.staticAccountKeys.length;
  const latency = Date.now() - firstShredTs;
  console.log(
    `slot=${slot} sig=${sig} ixs=${numIxs} keys=${numKeys} +${latency}ms`,
  );
});

listener.start();

// Periodic counters
setInterval(() => {
  const l = listener.stats();
  const p = parser.stats();
  console.log(
    `[stats] packets=${l.packets} txStreamed=${p.txStreamed} ` +
    `slotsOk=${p.slotsOk} slotsFail=${p.slotsFail}`,
  );
}, 10_000);
