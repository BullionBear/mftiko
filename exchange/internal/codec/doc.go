// Package codec is what every exchange adapter reads and writes a venue's wire format with (DECISIONS E03-D002,
// E00-D008): the two frozen sonic configurations, the number type, request signing and query encoding. An
// adapter uses these and does not configure sonic, sign or build a query on its own.
//
//   - JSON in: DecodeFrame for a websocket frame borrows, so a decoded string may point into the frame and is
//     valid only while the frame is. DecodeBody for a REST response copies every string, because the
//     transport reuses the response. Both match field names case-sensitively: Binance sends "b" and "B",
//     "e" and "E" as different fields.
//   - JSON out: Marshal, once per request; what is signed is what is sent.
//   - Numbers: a Decimal field reads the venue's text with decimal.ParseBytes from the input itself.
//   - Signing: a Signer holds an api_id's HMAC-SHA256 secret or ed25519 key, parsed once, and appends
//     signatures in hex or base64. It never formats its key, and neither do its errors.
//   - Query: Query appends percent-encoded pairs in the order given, without url.Values.
package codec
