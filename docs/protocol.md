# Wire protocol v1

Integers are big-endian. Size is the **actual complete UDP payload**, not the IP MTU.
The magic `0x5550` and request/response values 1/2 are choices of this implementation.
The original tutorial's 23-byte request format is preserved.

| Offset | Bytes | Field |
|---|---:|---|
| 0 | 2 | protocol = 0x5550 (`UP`) |
| 2 | 1 | version = 1 |
| 3 | 1 | type: request = 1, response = 2 |
| 4 | 1 | flags |
| 5 | 8 | seq: u64 |
| 13 | 8 | send_ts: i64, client Unix nanoseconds |
| 21 | 2 | size: u16, actual datagram length |
| 23 | 8 if flagged | token: u64 |
| 23 or 31 | remainder | padding |

Flags: `0x01` ECHO_SMALL, `0x02` NO_PADDING, `0x04` TOKEN_PRESENT. Unknown bits are
rejected. TOKEN_PRESENT must agree with token storage. NO_PADDING requires size to
be exactly the header length; supported by the codec but not exposed as a CLI flag.
ECHO_SMALL keeps the request full-sized but sets response size to 23/31. The response
preserves flags, seq, send_ts and token and sets type=2. All encoded padding is zero;
decoding accepts arbitrary padding. The server regenerates padding rather than
copying arbitrary request data.

Decoder checks lengths before slicing, marker, version, type, flags and declared size.
The maximum accepted payload is 65507; there is no unsafe struct-to-bytes conversion.
Response acceptance requires the target address, a successfully sent seq, matching
send_ts, flags, token and expected response size. The sender ensures increasing
send_ts within a trial even if the wall clock moves backwards. Durations use monotonic
clocks. A new socket is created for each trial.

Token is an optional plaintext shared marker, not a MAC. No claim of authenticated
measurements or replay protection against an active observer is made.
