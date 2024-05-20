mod resp;
use resp::Value;
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

async fn handle_client(stream: TcpStream) {
    let mut handler = resp::RespHandler::new(stream);
    loop {
        let value = handler.read_value().await.unwrap();
        println!("Got value {:?}", value);
        let response = if let Some(v) = value {
            let (command, args) = extract_command(v).unwrap();
            println!("Command: {}, Args: {:?}", command, args);
            match command.as_str() {
                "PING" => Value::SimpleString("PONG".to_string()),
                "ECHO" => args.first().unwrap().clone(),
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
    // write basic event loop with for loop
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                tokio::spawn(async move { handle_client(stream).await});
            }
            Err(e) => {
                eprintln!("Error: {:?}", e);
            }
        }
    }
}