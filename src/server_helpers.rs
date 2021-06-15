use bytes::{Buf, Bytes, BytesMut};
use std::io::{Cursor, Error, ErrorKind, Result};
use futures::future::{BoxFuture, FutureExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug)]
pub enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
}

pub struct Connection {
    pub stream: TcpStream,
    pub buf: BytesMut,
}

#[derive(Debug)]
pub enum Command {
    Get { key: String },
    Set { key: String, value: String },
    Delete { key: String },
    Update { key: String, value: String },
}

pub fn get_line(src: &mut Cursor<&[u8]>) -> Result<Bytes> {
    let len = src.remaining();
    let mut buf: Vec<u8> = vec![];
    for _ in 0..len {
        let ch = src.get_u8();
        if ch == b'\r' && src.has_remaining() {
            let next = src.get_u8();
            if next == b'\n' {
                return Ok(Bytes::from(buf));
            } else {
                return Err(Error::new(ErrorKind::InvalidData, ""));
            }
        }
        buf.push(ch);
    }
    Err(Error::new(ErrorKind::InvalidData, ""))
}

impl Frame {
    pub fn check(src: &mut Cursor<&[u8]>) -> Result<()> {
        if !src.has_remaining() {
            return  Err(Error::new(ErrorKind::InvalidData, ""))
        }
        let res = match src.get_u8() {
            b'+' => {
                get_line(src)?;
                Ok(())
            }
            b':' => {
                get_line(src)?;
                Ok(())
            }
            b'$' => {
                let len_bytes = get_line(src)?;
                let len_str = String::from_utf8_lossy(&len_bytes);
                if len_str.eq("-1") {
                    return Ok(())
                }
                let len = len_str.parse::<u32>();
                let res = match len {
                    Ok(len) => {
                        let bytes = get_line(src)?;
                        if bytes.len() == len as usize {
                            return Ok(());
                        }
                        Err(Error::new(ErrorKind::InvalidData, ""))
                    }
                    Err(_) => Err(Error::new(ErrorKind::InvalidData, "")),
                };
                res
            }
            b'*' => {
                let len_bytes = get_line(src)?;
                let len_str = String::from_utf8_lossy(&len_bytes);
                if len_str.eq("-1") {
                    return Ok(())
                }
                let len = len_str.parse::<u32>();

                let res = match len {
                    Ok(len) => {
                        let mut res = Vec::<Result<()>>::with_capacity(len as usize);
                        for _ in 0..len {
                            let check = Frame::check(src);
                            res.push(check);
                        }
                        let filtered = res
                            .into_iter()
                            .filter(|res| res.is_ok())
                            .collect::<Vec<Result<()>>>();
                        if filtered.len() == len as usize {
                            return Ok(());
                        }
                        return Err(Error::new(ErrorKind::InvalidData, ""));
                    }
                    Err(_) => Err(Error::new(ErrorKind::InvalidData, "")),
                };
                res
            }
            b'-' => {
                get_line(src)?;
                Ok(())
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "")),
        };
        res
    }
    pub fn parse(src: &mut Cursor<&[u8]>) -> Result<Frame> {
        let res = match src.get_u8() {
            b'+' => {
                let string = get_line(src)?;
                Ok(Frame::Simple(String::from_utf8_lossy(&string).to_string()))
            }
            b':' => {
                let string = get_line(src)?;
                let res = String::from_utf8_lossy(&string).to_string().parse::<u64>();
                match res {
                    Ok(int) => Ok(Frame::Integer(int)),
                    Err(_) => Err(Error::new(ErrorKind::InvalidData, "")),
                }
            }
            b'$' => {
                let len_bytes = get_line(src)?;
                let len_str = String::from_utf8_lossy(&len_bytes);
                if len_str.eq("-1") {
                    return Ok(Frame::Null);
                }
                let len = len_str.parse::<u32>();
                let res = match len {
                    Ok(len) => {
                        let bytes = get_line(src)?;
                        if bytes.len() == len as usize {
                            return Ok(Frame::Bulk(bytes));
                        }
                        Err(Error::new(ErrorKind::InvalidData, ""))
                    }
                    Err(_) => Err(Error::new(ErrorKind::InvalidData, "")),
                };
                res
            }
            b'*' => {
                let len_bytes = get_line(src)?;
                let len_str = String::from_utf8_lossy(&len_bytes);
                if len_str.eq("-1") {
                    return Ok(Frame::Null);
                }
                let len = len_str.parse::<u32>();

                let res = match len {
                    Ok(len) => {
                        let mut res = Vec::<Frame>::with_capacity(len as usize);
                        for _ in 0..len {
                            let check = Frame::parse(src)?;
                            res.push(check);
                        }
                        Ok(Frame::Array(res))
                    }
                    Err(_) => Err(Error::new(ErrorKind::InvalidData, "")),
                };
                res
            }
            b'-' => {
                let bytes = get_line(src)?;
                Ok(Frame::Error(String::from_utf8_lossy(&bytes).to_string()))
            }
            _ => Err(Error::new(ErrorKind::InvalidData, "")),
        };
        res
    }
}

impl Command {
    pub fn from_frame(frame: &Frame) -> Result<Self> {
        match frame {
            Frame::Array(fr) => match fr.get(0).unwrap() {
                Frame::Bulk(b) if String::from_utf8_lossy(b) == "get" => {
                    if let Some(Frame::Bulk(key)) = fr.get(1) {
                        return Ok(Command::Get {
                            key: String::from_utf8_lossy(key).to_string(),
                        });
                    }
                    Err(Error::new(ErrorKind::InvalidData, ""))
                }
                Frame::Bulk(b) if String::from_utf8_lossy(b) == "set" => {
                    if let Some(Frame::Bulk(key)) = fr.get(1) {
                        if let Some(Frame::Bulk(value)) = fr.get(2) {
                            return Ok(Command::Set {
                                key: String::from_utf8_lossy(key).to_string(),
                                value: String::from_utf8_lossy(value).to_string(),
                            });
                        }
                        return Err(Error::new(ErrorKind::InvalidData, ""));
                    }
                    Err(Error::new(ErrorKind::InvalidData, ""))
                }
                Frame::Bulk(b) if String::from_utf8_lossy(b) == "delete" => {
                    if let Some(Frame::Bulk(key)) = fr.get(1) {
                        return Ok(Command::Delete {
                            key: String::from_utf8_lossy(key).to_string(),
                        });
                    }
                    Err(Error::new(ErrorKind::InvalidData, ""))
                }
                Frame::Bulk(b) if String::from_utf8_lossy(b) == "update" => {
                    if let Some(Frame::Bulk(key)) = fr.get(1) {
                        if let Some(Frame::Bulk(value)) = fr.get(2) {
                            return Ok(Command::Update {
                                key: String::from_utf8_lossy(key).to_string(),
                                value: String::from_utf8_lossy(value).to_string(),
                            });
                        }
                        return Err(Error::new(ErrorKind::InvalidData, ""));
                    }
                    Err(Error::new(ErrorKind::InvalidData, ""))
                }

                _ => Err(Error::new(ErrorKind::InvalidInput, "Unimplemented")),
            },

            _ => Err(Error::new(ErrorKind::InvalidInput, "Invalid frame")),
        }
    }

    pub fn key(&self) -> Option<String> {
        match self {
            Self::Get { key } | Self::Delete { key } => Some(key.clone()),
            Self::Set { key, value: _ } | Self::Update { key, value: _ } => Some(key.clone()),
        }
    }

    pub fn value(&self) -> Option<String> {
        match self {
            Self::Get { key: _ } | Self::Delete { key: _ } => None,
            Self::Set { key: _, value} | Self::Update { key:_, value} => Some(value.clone()),
        }
    }
}

impl Connection {
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            if let Ok(Some(frame)) = self.parse_frame() {
                return Ok(Some(frame));
            }
            match self.stream.read_buf(&mut self.buf).await? {
                n => {
                    if n == 0 && self.buf.is_empty() {
                        return Ok(None)
                    } else if n == 0 && !self.buf.is_empty() {
                        return Err(Error::new(
                            ErrorKind::ConnectionReset,
                            "Connection reset by peer",
                        ))
                    }
                }
            }
        }
 
    }

    pub fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut src = Cursor::new(&self.buf[..]);
        match Frame::check(&mut src) {
            Ok(()) => {
                let len = src.position();
                src.set_position(0);

                let frame = Frame::parse(&mut src)?;

                self.buf.advance(len as usize);

                Ok(Some(frame))
            }
            Err(err) => Err(err),
        }
    }

    pub fn write_frame(&mut self, frame: Frame) -> BoxFuture<'_, Result<()>> {
        async move {
            match frame {
                Frame::Array(vec) => {
                    let len = vec.len();
                    self.stream.write(format!("*{}\r\n", len).as_bytes()).await?;
                    for fr in vec {
                        self.write_frame(fr).await?;
                    }
                    self.stream.write("\r\n".as_bytes()).await?;
                }
                Frame::Bulk(bytes) => {
                    let len = bytes.len();
                    self.stream.write(format!("${}\r\n", len).as_bytes()).await?;
                    for byte in bytes {
                        self.stream.write(&[byte]).await?;
                    } 
                    self.stream.write("\r\n".as_bytes()).await?;
                }
                Frame::Error(err) => {
                    let bytes = err.as_bytes();
                    self.stream.write("-\r\n".as_bytes()).await?;
                    self.stream.write(bytes).await?;
                    self.stream.write("\r\n".as_bytes()).await?;
                }
                Frame::Integer(int) => {
                    self.stream.write(format!(":{}\r\n", int).as_bytes()).await?; 
                }
                Frame::Null => {
                    self.stream.write("\0\r\n".as_bytes()).await?;
                }
                Frame::Simple(msg) => {
                    let bytes = msg.as_bytes();
                    self.stream.write("+".as_bytes()).await?;
                    self.stream.write(bytes).await?;
                    self.stream.write("\r\n".as_bytes()).await?;
                }
            }
            self.stream.flush().await.unwrap();
            Ok(())
        }.boxed()
    }
        
}

    
