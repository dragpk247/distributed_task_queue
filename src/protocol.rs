use bytes::{Buf, BytesMut};
use std::io::{Error, ErrorKind};

#[derive(Debug, PartialEq, Clone)]
pub enum Command {
    Ping,
    Lpush {
        queue: String,
        payload: Vec<u8>,
    },
    Rpop {
        queue: String,
    },
    Rpoplpush {
        source: String,
        destination: String,
    },
    /// BRPOP queue [queue ...] timeout_seconds
    Brpop {
        queues: Vec<String>,
        timeout_secs: f64,
    },
    /// BRPOPLPUSH source destination timeout_seconds
    Brpoplpush {
        source: String,
        destination: String,
        timeout_secs: f64,
    },
    /// TASKACK queue task_id
    TaskAck {
        queue: String,
        task_id: String,
    },
    /// TASKNACK queue task_id
    TaskNack {
        queue: String,
        task_id: String,
    },
    /// Trigger background or inline AOF compaction
    BgRewriteAof,
    /// Connect as replica node streaming mutation log
    ReplConf,
    Sync,
    Unknown,
}

/// Parses a raw binary buffer to extract a valid, complete RESP command frame
pub fn parse_command(buffer: &mut BytesMut) -> Result<Option<(Command, Vec<u8>)>, Error> {
    if buffer.is_empty() {
        return Ok(None);
    }

    // Inspect the first byte to verify it's a RESP Array container
    if buffer[0] != b'*' {
        return Err(Error::new(
            ErrorKind::InvalidData,
            "Expected RESP Array marker '*'",
        ));
    }

    // Find the end of the array length indicator line (\r\n)
    let Some(line_end) = buffer.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None); // Incomplete frame line, wait for more data from the socket
    };

    // Parse how many arguments are contained in this command array
    let len_str = std::str::from_utf8(&buffer[1..line_end])
        .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 sequence in array length"))?;

    let num_elements: usize = len_str
        .parse()
        .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid integer string for array length"))?;

    // Keep track of our parsing cursor offset within the buffer
    let mut cursor = line_end + 2;
    let mut args: Vec<Vec<u8>> = Vec::with_capacity(num_elements);

    // Iteratively extract each individual Bulk String ($) from the array container
    for _ in 0..num_elements {
        if cursor >= buffer.len() {
            return Ok(None); // Incomplete stream, wait for next socket chunk
        }

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
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 in bulk string length"))?;
        let str_len: usize = str_len_str
            .parse()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid integer for bulk string length"))?;

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

    // Extract the raw command frame bytes before advancing buffer
    let raw_frame = buffer[..cursor].to_vec();

    // Safely advance the buffer to drop the processed command bytes from memory
    buffer.advance(cursor);

    // Map extracted bulk string payloads to concrete Rust structured variants
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
            Command::Rpoplpush { source, destination }
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
        _ => Command::Unknown,
    };

    Ok(Some((cmd, raw_frame)))
}

/// Helper functions to format RESP responses
pub fn resp_simple_string(msg: &str) -> Vec<u8> {
    format!("+{}\r\n", msg).into_bytes()
}

pub fn resp_error(msg: &str) -> Vec<u8> {
    format!("-ERR {}\r\n", msg).into_bytes()
}

pub fn resp_bulk_string(data: &[u8]) -> Vec<u8> {
    let mut resp = format!("${}\r\n", data.len()).into_bytes();
    resp.extend_from_slice(data);
    resp.extend_from_slice(b"\r\n");
    resp
}

pub fn resp_null() -> Vec<u8> {
    b"$-1\r\n".to_vec()
}

pub fn resp_integer(val: i64) -> Vec<u8> {
    format!(":{}\r\n", val).into_bytes()
}

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
}
