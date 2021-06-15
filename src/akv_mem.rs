use std::{env, path::{ Path }, sync::{Arc, Mutex}};
use bytes::{Bytes, BytesMut};
use tokio::{net::{TcpStream, TcpListener}};


use libactionkv::AKVMEM;
#[cfg(target_os = "windows")]
const USAGE: &str = "
USAGE:
    akv_mem.exe FILE 
    akv_mem.exe FILE 
    akv_mem.exe FILE 
    akv_mem.exe FILE 

";

#[cfg(not(target_os = "windows"))]
const USAGE: &str = "
USAGE:
    akv_mem FILE 
    akv_mem FILE 
    akv_mem FILE 
    akv_mem FILE 

";



#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let args: Vec<String>= env::args().collect();
    let fname = args.get(1).expect(&USAGE);

    let path = Path::new(fname);
    let mut store = libactionkv::open(path).expect("Unable to open path to database");
    store.load().expect("Unable to load data");
    let db = Arc::new(Mutex::new(store));
    loop {
        let (socket, _) = listener.accept().await.unwrap();
      
        let db_clone = Arc::clone(&db);
        tokio::spawn(async move {
            process(socket, db_clone).await
        });

    }

}

async fn process(socket: TcpStream, db: Arc<Mutex<AKVMEM>>)  {
    use libactionkv::server_helpers::{Command, Connection, Frame};

    let mut connection = Connection{ stream: socket, buf: BytesMut::with_capacity(4096)};

    while let Ok(Some(frame)) = connection.read_frame().await {
        let cmd = Command::from_frame(&frame).unwrap();
        let res = match db.lock().as_mut() {
            Ok(db) => {
                match cmd {
                    Command::Set{key, value} => {
                        match db.insert(&key.as_bytes().to_vec(), &value.as_bytes().to_vec()) {
                            Ok(()) => Frame::Simple("OK".to_string()),
                            Err(err) => Frame::Error(err.to_string())
                        }
                    }
                    Command::Get{ key } => {
                        let val = db.get(&key.as_bytes().to_vec());
                        match  val {
                            Ok(Some(val)) => {
                                Frame::Bulk(Bytes::from(val))
                            }
                            Ok(None) => Frame::Null,
                            Err(err) => Frame::Error(err.to_string())
                        }
                    }
                    Command::Delete{key} => {
                        match db.delete(&key.as_bytes().to_vec()) {
                            Ok(()) => Frame::Simple("OK".to_string()),
                            Err(err) => Frame::Error(err.to_string())
                        }
                        
                    }
                    Command::Update{key, value} => {
                        match db.update(&key.as_bytes().to_vec(), &value.as_bytes().to_vec()) {
                            Ok(()) => Frame::Simple("OK".to_string()),
                            Err(err) => Frame::Error(err.to_string())
                        }
                    }
                }
            }
            Err(err) => Frame::Error(err.to_string())
        };
        connection.write_frame(res).await.unwrap();
        
    }
}



