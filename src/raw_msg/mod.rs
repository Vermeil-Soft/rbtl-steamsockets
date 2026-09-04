//! Module for encoding Raw Msg in bytes over the network
//!
//! Basically Raw Msg -> bytes -> Raw Msg with no external data should work
//! 
//! Raw Msg structure has 3 parts:
//! 
//! * len of the newly acked ids ( -> stored as `ack_len`): 1 byte / u8, with 16 max per message
//! * `ack_len` ack ids as u32, for the ones we confirmed having received. `l` * 4 bytes size
//! * `ack_id` of the current message, 4 bytes / u32
//! * len of received data (stored as `data_len`)
//! * the data (`data_len` len)
//! 
//! Note that we don't need to send flags such as if this message is reliable or not, steam already
//! allows us to know if a message is reliable when we receive it.

use std::collections::VecDeque;

use crate::common::SeqId;

#[derive(Clone, Copy)]
pub (crate) struct RawMsgCommon {
    pub (crate) ack_len: usize,
    pub (crate) raw_acks: [SeqId; Self::ACK_MAX_LEN],
    pub (crate) has_data: bool,
    pub (crate) seq_id: SeqId,
}

impl RawMsgCommon {
    const ACK_MAX_LEN: usize = 8;

    pub (crate) fn acks(&self) -> &[SeqId] {
        &self.raw_acks[0..self.ack_len as usize]
    }
}

pub (crate) struct RawMsgOut<'a> {
    pub (crate) common: RawMsgCommon,
    pub (crate) data: &'a [u8],
}

impl<'a> RawMsgOut<'a> {

    pub fn new(confirmed_acks: &mut VecDeque<SeqId>, seq_id: SeqId, data: &'a [u8]) -> Self {
        let mut i: usize = 0;
        let mut common = RawMsgCommon {
            ack_len: 0,
            raw_acks: Default::default(),
            has_data: data.len() > 0,
            seq_id
        };
        while i < RawMsgCommon::ACK_MAX_LEN {
            let Some(ack_seq_id) = confirmed_acks.pop_front() else {
                break;
            };
            common.raw_acks[i] = ack_seq_id;
            i += 1;
        }
        common.ack_len = i;

        Self { common, data }
    }

    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        buf.clear();
        if self.common.has_data {
            // ack_len + acks + seq_id + data
            buf.resize(1 + self.common.ack_len as usize * std::mem::size_of::<u32>() + 4 + self.data.len(), 0u8);
        } else {
            buf.resize(1 + self.common.ack_len as usize * std::mem::size_of::<u32>(), 0u8);
        }

        let ack_len = self.common.ack_len;

        // encode ack_len
        buf[0] = ack_len as u8;

        // encode acks
        for i in 0..ack_len {
            let offset = 1 + i * std::mem::size_of::<u32>();
            let be_bytes = self.common.raw_acks[i].to_be_bytes();
            buf[offset..offset+4].copy_from_slice(&be_bytes);
        }

        if self.common.has_data {
            let seq_id_offset = 1 + ack_len * std::mem::size_of::<u32>();

            buf[seq_id_offset .. seq_id_offset + 4].copy_from_slice(&self.common.seq_id.to_be_bytes());

            let data_offset = seq_id_offset + 4;
            buf[data_offset .. data_offset + self.data.len()].copy_from_slice(&self.data);
        }
    }
} 

pub (crate) struct RawMsgIn {
    pub (crate) common: RawMsgCommon,
}

impl RawMsgIn {
    /// Decode raw bytes to retrieve ack ids, seq id, etc
    /// 
    /// Warning clears buf before filling it
    pub fn decode_from(buf: &mut Vec<u8>, data: &[u8]) -> Option<RawMsgIn> {
        let mut common = RawMsgCommon {
            ack_len: 0,
            raw_acks: [0; RawMsgCommon::ACK_MAX_LEN],
            has_data: false,
            seq_id: 0,
        };
        common.ack_len = *data.get(0)? as usize;
        let seq_id_offset = 1+ common.ack_len * std::mem::size_of::<u32>();
        let acks = data.get(1..seq_id_offset)?;
        let (chunks, _rest) = acks.as_chunks::<4>();
        for (i, chunk) in chunks.iter().enumerate() {
            let ack_seq_id = u32::from_be_bytes(*chunk);
            common.raw_acks[i] = ack_seq_id;
        }

        if data.len() <= seq_id_offset + 4 {
            // no data
            return Some(Self { common })
        }

        common.has_data = true;
        let seq_id_bytes = data[seq_id_offset .. seq_id_offset + 4].as_array::<4>()?;
        common.seq_id = u32::from_be_bytes(*seq_id_bytes);

        let data_offset = seq_id_offset + 4;
        let data_len = data.len().saturating_sub(data_offset);
        buf.clear();
        buf.resize(data_len, 0);
        buf.copy_from_slice(&data[data_offset..data_offset + data_len]);
        Some(Self { common })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::SeqId;

    fn test_ser_deser(acks: &[SeqId], has_data: bool, seq_id: SeqId, data: &[u8]) {
        let common = RawMsgCommon {
            ack_len: acks.len(),
            raw_acks: [0; RawMsgCommon::ACK_MAX_LEN],
            has_data,
            seq_id
        };
        let mut raw_bytes = Vec::new();
        let msg_out = RawMsgOut { common, data };
        msg_out.encode_into(&mut raw_bytes);
        let mut buf_in = Vec::new();
        let raw_msg_in = RawMsgIn::decode_from(&mut buf_in, &raw_bytes).unwrap();
        assert_eq!(raw_msg_in.common.ack_len, common.ack_len);
        assert_eq!(&raw_msg_in.common.raw_acks, &common.raw_acks);
        assert_eq!(raw_msg_in.common.has_data, common.has_data);
        assert_eq!(raw_msg_in.common.seq_id, common.seq_id);
        assert_eq!(buf_in, data);
    }

    #[test]
    fn test_encode_decode_no_acks_no_data() {
        test_ser_deser(&[], false, 0, &[]);
    }

    #[test]
    fn test_encode_decode_with_acks_no_data() {
        test_ser_deser(&[1, 2, 3], false, 0, &[]);
    }

    #[test]
    fn test_encode_decode_no_acks_with_data() {
        test_ser_deser(&[], true, 4, &[1, 2, 3, 4]);
    }

    #[test]
    fn test_encode_decode_with_acks_and_data() {
        test_ser_deser(&[1, 2, 3], true, 4, &[1, 2, 3, 4]);
    }

    #[test]
    fn test_encode_decode_max_acks_with_data() {
        test_ser_deser(&[1, 2, 3, 4, 5, 6, 7, 8], true, 4, &[1, 2, 3, 4]);
    }
}