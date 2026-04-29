// Emit only transactions that touch a specific program ID.
//
//   node examples/filter-by-program.js <programId> [port]
//
// e.g. Jupiter v6 aggregator:
//   node examples/filter-by-program.js JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4
//
// Note: this filters on STATIC account keys only. Account keys loaded via
// Address Lookup Tables are not visible here without resolving the ALTs first.

import { ShredListener, ShredParser } from '../src/index.js';

const programId = process.argv[2];
const port = Number(process.argv[3] ?? 20000);

if (!programId) {
  console.error('Usage: filter-by-program.js <programId> [port]');
  process.exit(1);
}

const listener = new ShredListener({ port });
const parser   = new ShredParser();

listener.on('shred', (msg) => parser.ingest(msg));
listener.on('error', (err) => console.error('[listener]', err));

let matched = 0;
parser.on('tx', ({ slot, tx, firstShredTs }) => {
  const keys = tx.message.staticAccountKeys;
  let touchesProgram = false;
  for (const k of keys) {
    if (k.toBase58() === programId) { touchesProgram = true; break; }
  }
  if (!touchesProgram) return;

  matched++;
  const sig = Buffer.from(tx.signatures[0]).toString('hex').slice(0, 16);
  const latency = Date.now() - firstShredTs;
  console.log(`#${matched} slot=${slot} sig=${sig} +${latency}ms`);
});

listener.start();
console.log(`filtering for program ${programId} on UDP :${port}`);
