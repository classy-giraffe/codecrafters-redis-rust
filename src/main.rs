use std::{io::{Read, Write}, net::{TcpListener, TcpStream}, thread};
use std::io;

fn handle_client(mut stream: TcpStream) -> io::Result<()> {
    let mut buffer = [0; 512]; // Use a fixed-size buffer for reading

    loop {
        let bytes_read = stream.read(&mut buffer)?;
    
        if bytes_read == 0 {
            break; // Connection closed
        }
    
        stream.write_all(b"+PONG\r\n")?;
        stream.flush()?;
    }

    Ok(())
}

fn main() -> io::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:6379")?;

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}
