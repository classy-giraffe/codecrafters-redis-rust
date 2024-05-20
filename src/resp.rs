use anyhow::{Ok, Result};
use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Clone, Debug)]
pub enum Value {
    Array(Vec<Value>),
    BulkString(String),
    SimpleString(String),
}

impl Value {
    pub fn serialise(self) -> String {
        match self {
            Value::BulkString(b) => format!("${}\r\n{}\r\n", b.chars().count(), b),
            Value::SimpleString(s) => format!("+{}\r\n", s),
            _ => panic!("Unsupported value for serialisation"),
        }
    }
}

pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        RespHandler {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
        if bytes_read == 0 {
            return Ok(None);
        }
        let (v, _) = parse_msg(self.buffer.split())?;
        Ok(Some(v))
    }

    pub async fn write_value(&mut self, value: Value) -> Result<()> {
        self.stream.write(value.serialise().as_bytes()).await?;
        Ok(())
    }
}

fn read_until_term(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[0..(i - 1)], i + 1));
        }
    }
    return None;
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, length)) = read_until_term(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();
        return Ok((Value::SimpleString(string), length + 1));
    }
    return Err(anyhow::anyhow!("Invalid string {:?}" , buffer));
}

fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (bulk_str_len, bytes_read) = if let Some((line, len)) = read_until_term(&buffer[1..]) {
        let bulk_str_len = parse_int(line)?;
        (bulk_str_len, len + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array format {:?}", buffer));
    };

    let end_bulk_string = bytes_read + bulk_str_len as usize;
    let total_parsed = end_bulk_string + 2;

    Ok((Value::BulkString(String::from_utf8(buffer[bytes_read..end_bulk_string].to_vec())?), total_parsed))
}

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let (array_length, mut bytes_read) = if let Some((line, length)) = read_until_term(&buffer[1..]) {
        let array_length = parse_int(line)?;
        (array_length, length + 1)
    } else {
        return Err(anyhow::anyhow!("Invalid array length {:?}", buffer));
    };

    let mut array = vec![];
    for _ in 0..array_length {
        let (value, length) = parse_msg(BytesMut::from(&buffer[bytes_read..]))?;
        array.push(value);
        bytes_read += length;
    }

    return Ok((Value::Array(array), bytes_read));

}

fn parse_msg(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '$' => parse_bulk_string(buffer),
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        _ => Err(anyhow::anyhow!("Unsupported message type {:?}", buffer)),
    }
}
