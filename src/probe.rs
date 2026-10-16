//! Version 1 wire format. All integers are big-endian; `size` includes this header.
use thiserror::Error;

pub const BASE_HEADER_SIZE: u16 = 23;
pub const MAX_PAYLOAD: u16 = 65_507;
pub const PROTOCOL: u16 = 0x5550;
pub const VERSION: u8 = 1;
pub const ECHO_SMALL: u8 = 1;
pub const NO_PADDING: u8 = 2;
pub const TOKEN_PRESENT: u8 = 4;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ProbeError {
    #[error("payload size {size} is smaller than header size {minimum}")]
    PayloadTooSmall { size: u16, minimum: u16 },
    #[error("payload size {0} exceeds the supported UDP maximum of 65507")]
    PayloadTooLarge(usize),
    #[error("truncated probe header")]
    Truncated,
    #[error("unknown protocol marker")]
    Protocol,
    #[error("unsupported protocol version {0}")]
    Version(u8),
    #[error("unknown packet type {0}")]
    Kind(u8),
    #[error("invalid flags or padding")]
    Flags,
    #[error("declared size {declared} differs from received size {actual}")]
    Size { declared: u16, actual: usize },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    Request = 1,
    Response = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub kind: Kind,
    pub flags: u8,
    pub seq: u64,
    pub send_ts: i64,
    pub size: u16,
    pub token: Option<u64>,
}

impl Packet {
    pub fn header_size(&self) -> u16 {
        BASE_HEADER_SIZE + if self.token.is_some() { 8 } else { 0 }
    }

    pub fn validate(&self) -> Result<(), ProbeError> {
        if self.size < self.header_size() {
            return Err(ProbeError::PayloadTooSmall {
                size: self.size,
                minimum: self.header_size(),
            });
        }
        if self.size > MAX_PAYLOAD {
            return Err(ProbeError::PayloadTooLarge(self.size.into()));
        }
        if self.flags & !(ECHO_SMALL | NO_PADDING | TOKEN_PRESENT) != 0
            || (self.flags & TOKEN_PRESENT != 0) != self.token.is_some()
            || (self.flags & NO_PADDING != 0 && self.size != self.header_size())
        {
            return Err(ProbeError::Flags);
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProbeError> {
        self.validate()?;
        let mut bytes = vec![0; usize::from(self.size)];
        bytes[0..2].copy_from_slice(&PROTOCOL.to_be_bytes());
        bytes[2] = VERSION;
        bytes[3] = self.kind as u8;
        bytes[4] = self.flags;
        bytes[5..13].copy_from_slice(&self.seq.to_be_bytes());
        bytes[13..21].copy_from_slice(&self.send_ts.to_be_bytes());
        bytes[21..23].copy_from_slice(&self.size.to_be_bytes());
        if let Some(token) = self.token {
            bytes[23..31].copy_from_slice(&token.to_be_bytes());
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProbeError> {
        if bytes.len() < usize::from(BASE_HEADER_SIZE) {
            return Err(ProbeError::Truncated);
        }
        if bytes.len() > usize::from(MAX_PAYLOAD) {
            return Err(ProbeError::PayloadTooLarge(bytes.len()));
        }
        if bytes[0..2] != PROTOCOL.to_be_bytes() {
            return Err(ProbeError::Protocol);
        }
        if bytes[2] != VERSION {
            return Err(ProbeError::Version(bytes[2]));
        }
        let kind = match bytes[3] {
            1 => Kind::Request,
            2 => Kind::Response,
            other => return Err(ProbeError::Kind(other)),
        };
        // Every fixed-size conversion is preceded by a length check.
        let token = if bytes[4] & TOKEN_PRESENT != 0 {
            Some(u64::from_be_bytes(
                bytes
                    .get(23..31)
                    .ok_or(ProbeError::Truncated)?
                    .try_into()
                    .map_err(|_| ProbeError::Truncated)?,
            ))
        } else {
            None
        };
        let packet = Self {
            kind,
            flags: bytes[4],
            seq: u64::from_be_bytes(bytes[5..13].try_into().map_err(|_| ProbeError::Truncated)?),
            send_ts: i64::from_be_bytes(
                bytes[13..21]
                    .try_into()
                    .map_err(|_| ProbeError::Truncated)?,
            ),
            size: u16::from_be_bytes(
                bytes[21..23]
                    .try_into()
                    .map_err(|_| ProbeError::Truncated)?,
            ),
            token,
        };
        if usize::from(packet.size) != bytes.len() {
            return Err(ProbeError::Size {
                declared: packet.size,
                actual: bytes.len(),
            });
        }
        packet.validate()?;
        Ok(packet)
    }

    pub fn response(&self) -> Self {
        let mut packet = self.clone();
        packet.kind = Kind::Response;
        if self.flags & ECHO_SMALL != 0 {
            packet.size = packet.header_size();
        }
        packet
    }
}

/// Validated request parameters; preserves the initial tutorial API.
#[derive(Debug)]
pub struct Probe {
    seq: u64,
    size: u16,
}
impl Probe {
    pub fn new(seq: u64, size: u16) -> Result<Self, ProbeError> {
        let probe = Self { seq, size };
        probe.packet(0).validate()?;
        Ok(probe)
    }
    pub fn seq(&self) -> u64 {
        self.seq
    }
    pub fn payload_size(&self) -> usize {
        self.size.into()
    }
    fn packet(&self, send_ts: i64) -> Packet {
        Packet {
            kind: Kind::Request,
            flags: 0,
            seq: self.seq,
            send_ts,
            size: self.size,
            token: None,
        }
    }
    pub fn encode_request(&self, send_ts: i64) -> Vec<u8> {
        self.packet(send_ts)
            .encode()
            .expect("Probe constructor validates immutable fields")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_payload_equal_to_header_size() {
        let probe =
            Probe::new(1, BASE_HEADER_SIZE).expect("payload equal to header size should be valid");

        assert_eq!(probe.seq, 1);
        assert_eq!(probe.payload_size(), usize::from(BASE_HEADER_SIZE));
    }

    #[test]
    fn rejects_payload_smaller_than_header() {
        let result = Probe::new(1, BASE_HEADER_SIZE - 1);

        match result {
            Err(ProbeError::PayloadTooSmall { size, minimum }) => {
                assert_eq!(size, BASE_HEADER_SIZE - 1);
                assert_eq!(minimum, BASE_HEADER_SIZE);
            }
            other => panic!("expected PayloadTooSmall, got {other:?}"),
        }
    }

    #[test]
    fn encodes_request_header_and_padding() {
        let probe = Probe::new(0x0102030405060708, 25).expect("valid payload size");

        let bytes = probe.encode_request(0x1112131415161718);

        let expected_header: [u8; 23] = [
            0x55, 0x50, // protocol
            0x01, // version
            0x01, // request
            0x00, // flags
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, // seq
            0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, // send_ts
            0x00, 0x19, // size = 25
        ];

        assert_eq!(bytes.len(), 25);
        assert_eq!(&bytes[..23], &expected_header[..]);
        assert_eq!(&bytes[23..], &[0_u8, 0]);
    }
}
