//! # Protocol Module: RESP Wire Protocol Parser & Serializers
//!
//! Provides zero-copy parsing of the Redis Serialization Protocol (RESP) wire format
//! into structured Rust [`Command`] variants, along with helper response serializers.

use bytes::{Buf, BytesMut};
use std::io::{Error, ErrorKind};

/// Supported client-to-server commands.
#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    /// Health-check probe (`PING`). Responds with `+PONG\r\n`.
    Ping,

    /// Push an item to the head of the queue (`LPUSH <queue> <payload>`).
    Lpush { queue: String, payload: Vec<u8> },

    /// Non-blocking pop from the tail of the queue (`RPOP <queue>`).
    Rpop { queue: String },

    /// Atomically pops from the tail of source queue and prepends to destination queue (`RPOPLPUSH <source> <destination>`).
    Rpoplpush { source: String, destination: String },

    /// Non-busy blocking pop across one or more queues (`BRPOP <queue...> <timeout_secs>`).
    Brpop {
        queues: Vec<String>,
        timeout_secs: f64,
    },

    /// Blocking pop from source and push to destination (`BRPOPLPUSH <source> <destination> <timeout_secs>`).
    Brpoplpush {
        source: String,
        destination: String,
        timeout_secs: f64,
    },

    /// Pop and lease a task with a visibility timeout window (`RPOPLEASE <queue> [visibility_secs]`).
    /// Returns `[task_id, payload]`.
    RpopLease { queue: String, visibility_secs: f64 },

    /// Blocking pop and lease with timeout (`BRPOPLEASE <queue> <timeout_secs> [visibility_secs]`).
    BrpopLease {
        queue: String,
        timeout_secs: f64,
        visibility_secs: f64,
    },

    /// Heartbeat command to extend an active task lease (`TASKTOUCH <queue> <task_id> [extend_secs]`).
    TaskTouch {
        queue: String,
        task_id: String,
        extend_secs: f64,
    },

    /// Acknowledge successful completion of a leased task (`TASKACK <queue> <task_id>`).
    TaskAck { queue: String, task_id: String },

    /// Negative-acknowledge a failed task, incrementing retries or escalating to DLQ (`TASKNACK <queue> <task_id>`).
    TaskNack { queue: String, task_id: String },

    /// Triggers online compaction of the Append-Only File (`BGREWRITEAOF`).
    BgRewriteAof,

    /// Replication handshake negotiation (`REPLCONF`).
    ReplConf,

    /// Subscribes replica nodes to the live binary mutation stream (`SYNC`).
    Sync,

    /// Push an item with execution delay (`LPUSH_DELAY <queue> <delay_secs> <payload>`).
    LpushDelay {
        queue: String,
        delay_secs: f64,
        payload: Vec<u8>,
    },

    /// Push an item with priority level (`LPUSH_PRIORITY <queue> <priority> <payload>`).
    LpushPriority {
        queue: String,
        priority: u8,
        payload: Vec<u8>,
    },

    /// Authenticate client connection (`AUTH <password>`).
    Auth { password: String },

    /// Diagnostic server and queue engine information (`INFO`).
    Info,

    /// Fallback for unrecognized commands or malformed argument counts.
    Unknown,
}

/// Parses a raw binary stream buffer to extract a valid, complete RESP command frame.
///
/// Returns:
/// - `Ok(Some((Command, Vec<u8>)))` if a complete frame was parsed, along with the raw bytes.
/// - `Ok(None)` if the buffer currently contains an incomplete frame (caller should read more bytes from TCP socket).
/// - `Err(Error)` if the frame violates the RESP protocol structure.
pub fn parse_command(buffer: &mut BytesMut) -> Result<Option<(Command, Vec<u8>)>, Error> {
    if buffer.is_empty() {
        return Ok(None);
    }

    // Step 1: Inspect the first byte to verify it's a RESP Array container ('*')
    if buffer[0] != b'*' {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Expected RESP Array marker '*'",
        ));
    }

    // Step 2: Locate the trailing CRLF for the array element count
    let Some(line_end) = buffer.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None); // Incomplete frame line, wait for more data from the socket
    };

    // Step 3: Parse how many arguments are contained in this command array
    let len_str = std::str::from_utf8(&buffer[1..line_end]).map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "Invalid UTF-8 sequence in array length",
        )
    })?;

    let num_elements: usize = len_str.parse().map_err(|_| {
        Error::new(
            ErrorKind::InvalidData,
            "Invalid integer string for array length",
        )
    })?;

    // Step 4: Iteratively extract each individual Bulk String ($) from the array container
    let mut cursor = line_end + 2;
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(num_elements);

    for _ in 0..num_elements {
        if cursor >= buffer.len() {
            return Ok(None); // Incomplete stream, wait for next socket chunk
        }

        // Verify bulk string marker '$'
        if buffer[cursor] != b'$' {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Expected Bulk String marker '$'",
            ));
        }

        let Some(str_len_end) = buffer[cursor..].windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let absolute_str_len_end = cursor + str_len_end;

        let str_len_str = std::str::from_utf8(&buffer[(cursor + 1)..absolute_str_len_end])
            .map_err(|_| {
                Error::new(
                    ErrorKind::InvalidData,
                    "Invalid UTF-8 in bulk string length",
                )
            })?;
        let str_len: usize = str_len_str.parse().map_err(|_| {
            Error::new(
                ErrorKind::InvalidData,
                "Invalid integer for bulk string length",
            )
        })?;

        let string_data_start = absolute_str_len_end + 2;
        let string_data_end = string_data_start + str_len;

        // Ensure the full bulk string bytes and trailing \r\n are present in the buffer
        if string_data_end + 2 > buffer.len() {
            return Ok(None);
        }

        if &buffer[string_data_end..(string_data_end + 2)] != b"\r\n" {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Missing terminating CRLF for bulk string payload",
            ));
        }

        // Extract the raw payload data bytes
        let raw_payload = buffer[string_data_start..string_data_end].to_vec();
        args.push(raw_payload);

        cursor = string_data_end + 2;
    }

    // Step 5: Save raw frame bytes for persistence & replication before advancing the buffer
    let raw_frame = buffer[..cursor].to_vec();

    // Advance buffer cursor to discard processed command bytes from memory
    buffer.advance(cursor);

    // Step 6: Map extracted bulk string payloads to strongly-typed Command variants
    if args.is_empty() {
        return Ok(Some((Command::Unknown, raw_frame)));
    }

    let command_name = String::from_utf8_lossy(&args[0]).to_uppercase();
    let cmd = match command_name.as_str() {
        "PING" => Command::Ping,
        "LPUSH" if args.len() >= 3 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let payload = args[2].clone();
            Command::Lpush { queue, payload }
        }
        "RPOP" if args.len() == 2 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            Command::Rpop { queue }
        }
        "RPOPLPUSH" if args.len() == 3 => {
            let source = String::from_utf8_lossy(&args[1]).into_owned();
            let destination = String::from_utf8_lossy(&args[2]).into_owned();
            Command::Rpoplpush {
                source,
                destination,
            }
        }
        "BRPOP" if args.len() >= 3 => {
            let timeout_str = String::from_utf8_lossy(&args[args.len() - 1]);
            let timeout_secs = timeout_str.parse::<f64>().unwrap_or(0.0);
            let queues = args[1..args.len() - 1]
                .iter()
                .map(|q| String::from_utf8_lossy(q).into_owned())
                .collect();
            Command::Brpop {
                queues,
                timeout_secs,
            }
        }
        "BRPOPLPUSH" if args.len() == 4 => {
            let source = String::from_utf8_lossy(&args[1]).into_owned();
            let destination = String::from_utf8_lossy(&args[2]).into_owned();
            let timeout_str = String::from_utf8_lossy(&args[3]);
            let timeout_secs = timeout_str.parse::<f64>().unwrap_or(0.0);
            Command::Brpoplpush {
                source,
                destination,
                timeout_secs,
            }
        }
        "RPOPLEASE" if args.len() >= 2 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let visibility_secs = if args.len() >= 3 {
                String::from_utf8_lossy(&args[2])
                    .parse::<f64>()
                    .unwrap_or(30.0)
            } else {
                30.0
            };
            Command::RpopLease {
                queue,
                visibility_secs,
            }
        }
        "BRPOPLEASE" if args.len() >= 3 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let timeout_secs = String::from_utf8_lossy(&args[2])
                .parse::<f64>()
                .unwrap_or(0.0);
            let visibility_secs = if args.len() >= 4 {
                String::from_utf8_lossy(&args[3])
                    .parse::<f64>()
                    .unwrap_or(30.0)
            } else {
                30.0
            };
            Command::BrpopLease {
                queue,
                timeout_secs,
                visibility_secs,
            }
        }
        "TASKTOUCH" if args.len() >= 3 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let task_id = String::from_utf8_lossy(&args[2]).into_owned();
            let extend_secs = if args.len() >= 4 {
                String::from_utf8_lossy(&args[3])
                    .parse::<f64>()
                    .unwrap_or(30.0)
            } else {
                30.0
            };
            Command::TaskTouch {
                queue,
                task_id,
                extend_secs,
            }
        }
        "TASKACK" if args.len() == 3 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let task_id = String::from_utf8_lossy(&args[2]).into_owned();
            Command::TaskAck { queue, task_id }
        }
        "TASKNACK" if args.len() == 3 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let task_id = String::from_utf8_lossy(&args[2]).into_owned();
            Command::TaskNack { queue, task_id }
        }
        "BGREWRITEAOF" => Command::BgRewriteAof,
        "SYNC" => Command::Sync,
        "REPLCONF" => Command::ReplConf,
        "LPUSH_DELAY" | "LPUSHDELAY" if args.len() == 4 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let delay_str = String::from_utf8_lossy(&args[2]);
            match delay_str.parse::<f64>() {
                Ok(delay_secs) => {
                    let payload = args[3].clone();
                    Command::LpushDelay {
                        queue,
                        delay_secs,
                        payload,
                    }
                }
                Err(_) => Command::Unknown,
            }
        }
        "LPUSH_PRIORITY" | "LPUSHPRIORITY" if args.len() == 4 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let prio_str = String::from_utf8_lossy(&args[2]);
            match prio_str.parse::<u8>() {
                Ok(priority) => {
                    let payload = args[3].clone();
                    Command::LpushPriority {
                        queue,
                        priority,
                        payload,
                    }
                }
                Err(_) => Command::Unknown,
            }
        }
        "AUTH" if args.len() == 2 => {
            let password = String::from_utf8_lossy(&args[1]).into_owned();
            Command::Auth { password }
        }
        "INFO" => Command::Info,
        _ => Command::Unknown,
    };

    Ok(Some((cmd, raw_frame)))
}

/// Serializes a RESP Simple String (`+<msg>\r\n`)
pub fn resp_simple_string(msg: &str) -> Vec<u8> {
    format!("+{}\r\n", msg).into_bytes()
}

/// Serializes a RESP Error (`-ERR <msg>\r\n` or custom error prefix)
pub fn resp_error(msg: &str) -> Vec<u8> {
    if msg.starts_with("ERR ") || msg.starts_with("WRONGPASS ") || msg.starts_with("NOAUTH ") {
        format!("-{}\r\n", msg).into_bytes()
    } else {
        format!("-ERR {}\r\n", msg).into_bytes()
    }
}

/// Serializes a RESP Bulk String (`$<len>\r\n<data>\r\n`)
pub fn resp_bulk_string(data: &[u8]) -> Vec<u8> {
    let mut resp = format!("${}\r\n", data.len()).into_bytes();
    resp.extend_from_slice(data);
    resp.extend_from_slice(b"\r\n");
    resp
}

/// Serializes a RESP Null Bulk String (`$-1\r\n`) representing a nil/empty pop
pub fn resp_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

/// Serializes a RESP Integer (`:<val>\r\n`)
pub fn resp_integer(val: i64) -> Vec<u8> {
    format!(":{}\r\n", val).into_bytes()
}

/// Serializes a RESP Array of nested serialized elements (`*<count>\r\n...`)
pub fn resp_array(elements: &[Vec<u8>]) -> Vec<u8> {
    let mut resp = format!("*{}\r\n", elements.len()).into_bytes();
    for el in elements {
        resp.extend_from_slice(el);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ping() {
        let mut buf = BytesMut::from("*1\r\n$4\r\nPING\r\n");
        let (cmd, raw) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd, Command::Ping);
        assert_eq!(raw, b"*1\r\n$4\r\nPING\r\n");
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_lpush() {
        let mut buf = BytesMut::from("*3\r\n$5\r\nLPUSH\r\n$5\r\ntasks\r\n$11\r\nhello world\r\n");
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Lpush {
                queue: "tasks".to_string(),
                payload: b"hello world".to_vec()
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_rpop() {
        let mut buf = BytesMut::from("*2\r\n$4\r\nRPOP\r\n$5\r\ntasks\r\n");
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Rpop {
                queue: "tasks".to_string()
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_brpop() {
        let mut buf = BytesMut::from("*3\r\n$5\r\nBRPOP\r\n$5\r\ntasks\r\n$1\r\n5\r\n");
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Brpop {
                queues: vec!["tasks".to_string()],
                timeout_secs: 5.0
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_task_ack_and_nack() {
        let mut buf = BytesMut::from("*3\r\n$7\r\nTASKACK\r\n$5\r\ntasks\r\n$6\r\njob-42\r\n");
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::TaskAck {
                queue: "tasks".to_string(),
                task_id: "job-42".to_string(),
            }
        );

        let mut buf2 = BytesMut::from("*3\r\n$8\r\nTASKNACK\r\n$5\r\ntasks\r\n$6\r\njob-42\r\n");
        let (cmd2, _) = parse_command(&mut buf2).unwrap().unwrap();
        assert_eq!(
            cmd2,
            Command::TaskNack {
                queue: "tasks".to_string(),
                task_id: "job-42".to_string(),
            }
        );
    }

    #[test]
    fn test_partial_frame_returns_none() {
        let mut buf = BytesMut::from("*2\r\n$4\r\nRPOP\r\n$5\r\ntas");
        assert_eq!(parse_command(&mut buf).unwrap(), None);
        assert_eq!(buf.len(), 21); // Unconsumed
    }

    #[test]
    fn test_pipelined_commands() {
        let mut buf = BytesMut::from("*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n");
        let (cmd1, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd1, Command::Ping);
        let (cmd2, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(cmd2, Command::Ping);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_rpoplease_brpoplease_tasktouch() {
        let mut buf = BytesMut::from("*3\r\n$9\r\nRPOPLEASE\r\n$5\r\ntasks\r\n$2\r\n45\r\n");
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::RpopLease {
                queue: "tasks".to_string(),
                visibility_secs: 45.0
            }
        );

        let mut buf2 =
            BytesMut::from("*4\r\n$10\r\nBRPOPLEASE\r\n$5\r\ntasks\r\n$1\r\n5\r\n$2\r\n60\r\n");
        let (cmd2, _) = parse_command(&mut buf2).unwrap().unwrap();
        assert_eq!(
            cmd2,
            Command::BrpopLease {
                queue: "tasks".to_string(),
                timeout_secs: 5.0,
                visibility_secs: 60.0,
            }
        );

        let mut buf3 =
            BytesMut::from("*4\r\n$9\r\nTASKTOUCH\r\n$5\r\ntasks\r\n$6\r\njob-99\r\n$2\r\n15\r\n");
        let (cmd3, _) = parse_command(&mut buf3).unwrap().unwrap();
        assert_eq!(
            cmd3,
            Command::TaskTouch {
                queue: "tasks".to_string(),
                task_id: "job-99".to_string(),
                extend_secs: 15.0,
            }
        );
    }

    #[test]
    fn test_parse_lpush_delay() {
        let mut buf = BytesMut::from(
            "*4\r\n$11\r\nLPUSH_DELAY\r\n$5\r\ntasks\r\n$3\r\n2.5\r\n$11\r\nhello world\r\n",
        );
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::LpushDelay {
                queue: "tasks".to_string(),
                delay_secs: 2.5,
                payload: b"hello world".to_vec(),
            }
        );
        assert!(buf.is_empty());

        // Invalid delay format should map to Unknown
        let mut buf_err = BytesMut::from(
            "*4\r\n$11\r\nLPUSH_DELAY\r\n$5\r\ntasks\r\n$3\r\nabc\r\n$4\r\ntest\r\n",
        );
        let (cmd_err, _) = parse_command(&mut buf_err).unwrap().unwrap();
        assert_eq!(cmd_err, Command::Unknown);
    }

    #[test]
    fn test_parse_auth() {
        let mut buf = BytesMut::from("*2\r\n$4\r\nAUTH\r\n$8\r\nsecret42\r\n");
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::Auth {
                password: "secret42".to_string(),
            }
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn test_parse_lpush_priority() {
        // Test LPUSH_PRIORITY
        let mut buf = BytesMut::from(
            "*4\r\n$14\r\nLPUSH_PRIORITY\r\n$5\r\ntasks\r\n$2\r\n10\r\n$11\r\nhello world\r\n",
        );
        let (cmd, _) = parse_command(&mut buf).unwrap().unwrap();
        assert_eq!(
            cmd,
            Command::LpushPriority {
                queue: "tasks".to_string(),
                priority: 10,
                payload: b"hello world".to_vec(),
            }
        );
        assert!(buf.is_empty());

        // Test LPUSHPRIORITY (alias)
        let mut buf2 = BytesMut::from(
            "*4\r\n$13\r\nLPUSHPRIORITY\r\n$6\r\nurgent\r\n$1\r\n5\r\n$4\r\nfast\r\n",
        );
        let (cmd2, _) = parse_command(&mut buf2).unwrap().unwrap();
        assert_eq!(
            cmd2,
            Command::LpushPriority {
                queue: "urgent".to_string(),
                priority: 5,
                payload: b"fast".to_vec(),
            }
        );
        assert!(buf2.is_empty());

        // Invalid priority format should map to Unknown
        let mut buf_err = BytesMut::from(
            "*4\r\n$14\r\nLPUSH_PRIORITY\r\n$5\r\ntasks\r\n$3\r\nabc\r\n$4\r\ntest\r\n",
        );
        let (cmd_err, _) = parse_command(&mut buf_err).unwrap().unwrap();
        assert_eq!(cmd_err, Command::Unknown);

        // Priority overflow (> 255) should map to Unknown
        let mut buf_err2 = BytesMut::from(
            "*4\r\n$14\r\nLPUSH_PRIORITY\r\n$5\r\ntasks\r\n$3\r\n300\r\n$4\r\ntest\r\n",
        );
        let (cmd_err2, _) = parse_command(&mut buf_err2).unwrap().unwrap();
        assert_eq!(cmd_err2, Command::Unknown);
    }
}
