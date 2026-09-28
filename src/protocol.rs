use bytes::{Buf, BytesMut};
use std::io::{Error, ErrorKind};

#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Lpush { queue: String, payload: Vec<u8> },
    Rpop { queue: String },
    Ping,
    Unknown,
}

/// Parses a raw binary buffer to extract a valid, complete RESP command frame
pub fn parse_command(buffer: &mut BytesMut) -> Result<Option<Command>, Error> {
    if buffer.is_empty() {
        return Ok(None);
    }

    // Inspect the first byte to verify it's a RESP Array container
    if buffer[0] != b'*' {
        return Err(Error::new(ErrorKind::InvalidData, "Expected RESP Array marker '*'"));
    }

    // Find the end of the array length indicator line (\r\n)
    let Some(line_end) = buffer.windows(2).position(|w| w == b"\r\n") else {
        return Ok(None); // Incomplete frame line, wait for more data from the socket
    };

    // Parse how many arguments are contained in this command array
    let len_str = std::str::from_utf8(&buffer[1..line_end])
        .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 sequence in array length"))?;
    
    let num_elements: usize = len_str.parse()
        .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid integer string for array length"))?;

    // Keep track of our parsing cursor offset within the buffer
    let mut cursor = line_end + 2;
    let mut args = Vec::with_capacity(num_elements);

    // Iteratively extract each individual Bulk String ($) from the array container
    for _ in 0..num_elements {
        if cursor >= buffer.len() {
            return Ok(None); // Incomplete stream, wait for next socket chunk
        }

        if buffer[cursor] != b'$' {
            return Err(Error::new(ErrorKind::InvalidData, "Expected Bulk String marker '$'"));
        }

        let Some(str_len_end) = buffer[cursor..].windows(2).position(|w| w == b"\r\n") else {
            return Ok(None);
        };
        let absolute_str_len_end = cursor + str_len_end;

        let str_len_str = std::str::from_utf8(&buffer[(cursor + 1)..absolute_str_len_end])
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid UTF-8 in bulk string length"))?;
        let str_len: usize = str_len_str.parse()
            .map_err(|_| Error::new(ErrorKind::InvalidData, "Invalid integer for bulk string length"))?;

        let string_data_start = absolute_str_len_end + 2;
        let string_data_end = string_data_start + str_len;

        // Ensure the full bulk string bytes and trailing \r\n are present in the buffer
        if string_data_end + 2 > buffer.len() {
            return Ok(None);
        }

        if &buffer[string_data_end..(string_data_end + 2)] != b"\r\n" {
            return Err(Error::new(ErrorKind::InvalidData, "Missing terminating CRLF for bulk string payload"));
        }

        // Extract the raw payload data bytes
        let raw_payload = buffer[string_data_start..string_data_end].to_vec();
        args.push(raw_payload);

        cursor = string_data_end + 2;
    }

    // Safely advance the buffer to drop the processed command bytes from memory
    buffer.advance(cursor);

    // Map extracted bulk string payloads to concrete Rust structured variants
    if args.is_empty() {
        return Ok(Some(Command::Unknown));
    }

    let command_name = String::from_utf8_lossy(&args[0]).to_uppercase();
    match command_name.as_str() {
        "PING" => Ok(Some(Command::Ping)),
        "LPUSH" if args.len() == 3 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            let payload = args[2].clone();
            Ok(Some(Command::Lpush { queue, payload }))
        }
        "RPOP" if args.len() == 2 => {
            let queue = String::from_utf8_lossy(&args[1]).into_owned();
            Ok(Some(Command::Rpop { queue }))
        }
        _ => Ok(Some(Command::Unknown)),
    }
}
