// SPDX-License-Identifier: MIT

import dgram from 'node:dgram';
import { EventEmitter } from 'node:events';

/**
 * Default UDP receive buffer size in megabytes.
 *
 * Solana validators (and shred relays such as Jito ShredStream) emit traffic in
 * bursts that easily overflow the default ~256 KB kernel buffer, causing silent
 * packet loss. 128 MB matches typical mainnet shred bursts with headroom.
 */
export const DEFAULT_RECV_BUFFER_MB = 128;

/**
 * @typedef {object} ShredListenerOptions
 * @property {number} port            UDP port to bind to.
 * @property {string} [host]          Interface to bind to. Defaults to `0.0.0.0`.
 * @property {number} [recvBufferMb]  Kernel receive buffer size in MB. Defaults to {@link DEFAULT_RECV_BUFFER_MB}.
 *                                    The OS may cap this; on Linux you typically need
 *                                    `sysctl -w net.core.rmem_max=<bytes>` to raise the ceiling.
 */

/**
 * @typedef {object} ListenerStats
 * @property {number} packets  Total UDP packets received since `start()`.
 * @property {number} bytes    Total bytes received since `start()`.
 */

/**
 * UDP listener that emits raw shred packets as `Buffer`s.
 *
 * This class is intentionally minimal: it does not parse or filter shreds. It
 * simply binds a UDP socket, raises the kernel receive buffer to avoid drops
 * during bursts, and emits each datagram as a `'shred'` event. Pair with
 * {@link ShredParser} (or your own assembler) to reconstruct transactions.
 *
 * @example
 *   import { ShredListener } from 'solana-shred-parser';
 *
 *   const listener = new ShredListener({ port: 20000 });
 *   listener.on('shred', (msg) => console.log('shred', msg.length, 'bytes'));
 *   listener.on('error', (err) => console.error(err));
 *   listener.on('listening', ({ port, recvBufferMb }) => {
 *     console.log(`listening on :${port} (recv buffer ${recvBufferMb} MB)`);
 *   });
 *   listener.start();
 *
 * @fires ShredListener#shred       `(msg: Buffer)` — a raw shred datagram.
 * @fires ShredListener#listening   `({ port: number, host: string, recvBufferMb: number })` — socket bound.
 * @fires ShredListener#error       `(err: Error)` — socket error.
 * @fires ShredListener#close       `()` — socket closed.
 */
export class ShredListener extends EventEmitter {
  /** @param {ShredListenerOptions} options */
  constructor(options) {
    super();
    if (!options || typeof options.port !== 'number') {
      throw new TypeError('ShredListener: `port` is required and must be a number');
    }
    /** @readonly */
    this.port = options.port;
    /** @readonly */
    this.host = options.host ?? '0.0.0.0';
    /** @readonly */
    this.recvBufferMb = options.recvBufferMb ?? DEFAULT_RECV_BUFFER_MB;

    /** @private */
    this._socket = dgram.createSocket({ type: 'udp4' });
    /** @private */
    this._stats = { packets: 0, bytes: 0 };
    /** @private */
    this._started = false;
  }

  /**
   * Bind the UDP socket and begin emitting shreds. Calling `start()` more than
   * once throws — create a new instance instead.
   */
  start() {
    if (this._started) throw new Error('ShredListener: start() already called');
    this._started = true;

    this._socket.on('message', (msg) => {
      this._stats.packets++;
      this._stats.bytes += msg.length;
      this.emit('shred', msg);
    });

    this._socket.on('error', (err) => this.emit('error', err));
    this._socket.on('close', () => this.emit('close'));

    this._socket.on('listening', () => {
      let actualMb = 0;
      try {
        this._socket.setRecvBufferSize(this.recvBufferMb * 1024 * 1024);
        actualMb = Math.round(this._socket.getRecvBufferSize() / 1024 / 1024);
      } catch (err) {
        this.emit('error', err);
      }
      this.emit('listening', { port: this.port, host: this.host, recvBufferMb: actualMb });
    });

    this._socket.bind(this.port, this.host);
  }

  /** Close the underlying socket. Idempotent. */
  stop() {
    try { this._socket.close(); } catch { /* already closed */ }
  }

  /** @returns {ListenerStats} A snapshot of cumulative counters. */
  stats() {
    return { ...this._stats };
  }
}
