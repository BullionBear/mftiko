// Package okx is the OKX adapter: one venue, OKX, a unified account (D26). Spot, swap, futures and
// options share it; the instId tells them apart (BTC-USDT, BTC-USDT-SWAP).
//
// Facts the implementation must honor. Checked 2026-10-05 against okx.com/docs-v5; re-check before
// implementing.
//
//   - Routes: "public" (/ws/v5/public) and "business" (/ws/v5/business, candles among others). The
//     channel decides the route (D26). Private data is on /ws/v5/private, after login.
//   - No published cap on channels per connection: Budget is 0. What is limited: 480
//     subscribe/unsubscribe/login requests per connection per hour (SessionLimits.Rate), 64 KB of
//     channels per request (PerRequestBytes), 3 new connections per second per IP.
//   - Keepalive: send the text "ping" when nothing arrived for under 30s; expect "pong".
//   - Books are SnapshotStream: the first push after subscribe is the snapshot, then seqId/prevSeqId
//     chain, plus a crc32 checksum of the top levels (BookChecksum).
//   - Login signs timestamp + "GET" + "/users/self/verify" with HMAC-SHA256; the key needs a
//     passphrase (Credentials.Passphrase).
//   - Dead-man switch: cancel-all-after, account-wide.
//
// Shape only: every method that would talk to OKX returns exchange.ErrNotImplemented.
package okx
