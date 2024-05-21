mod resp;
mod storage;
use resp::Value;
use storage::Storage;
use tokio::net::{TcpListener, TcpStream};
use std::error::Error;
use anyhow::Result;

fn unpack_bulk_str(value: Value) -> Result<String> {
    match value {
        Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Expected command to be a bulk string"))
    }
}

fn extract_command(value: Value) -> Result<(String, Vec<Value>)> {
    match value {
        Value::Array(a) => {
            Ok((
                unpack_bulk_str(a.first().unwrap().clone())?,
                a.into_iter().skip(1).collect(),
            ))
        },
        _ => Err(anyhow::anyhow!("Unexpected command format")),
    }
}

async fn handle_client(stream: TcpStream, mut storage: Storage) {
    let mut handler = resp::RespHandler::new(stream);
    loop {
        let value = handler.read_value().await.unwrap();
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            println!("Command: {}, Args: {:?}", command, args);
            match command.as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
                "GET" => {
                    let key = unpack_bulk_str(args.first().unwrap().clone()).unwrap();
                    match storage.get(key) {
                        Some(value) => Value::BulkString(value.clone()),
                        None => Value::SimpleString("$-1\r\n".to_string()),
                    }
                },
                "SET" => {
                    let key = unpack_bulk_str(args.first().unwrap().clone()).unwrap();
                    let value = unpack_bulk_str(args.get(1).unwrap().clone()).unwrap();
                    storage.set(key, value);
                    Value::SimpleString("OK".to_string())
                },
                c => panic!("Cannot handle command {}", c),
            }
        } else {
            break;
        };
        println!("Sending value {:?}", response);
        handler.write_value(response).await.unwrap();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move { handle_client(stream, Storage::new()).await});
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
    }
}