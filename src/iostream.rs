use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

#[derive(Debug)]
pub enum StreamError {
    ReadAfterEof,
    UnexpectedEof,
    WriteAfterEof,
}

#[derive(Default)]
struct StreamBuffer {
    data: VecDeque<u8>,
    eof_written: bool,
}

impl StreamBuffer {
    fn is_eof(&self) -> bool {
        self.data.is_empty() && self.eof_written
    }
}

pub trait InputStream {
    fn read_byte(&self) -> Result<u8, StreamError>;
    fn read(&self, length: Option<usize>) -> Result<Vec<u8>, StreamError>;
    fn is_empty(&self) -> bool;
    fn len(&self) -> usize;
    fn eof_written(&self) -> bool;
    fn is_eof(&self) -> bool;
}

pub trait OutputStream {
    fn write_byte(&self, byte: u8) -> Result<(), StreamError>;
    fn write(&self, buf: &[u8]) -> Result<usize, StreamError>;
    fn write_eof(&self);
}

#[derive(Default)]
pub struct IoStream {
    data: Mutex<StreamBuffer>,
    cv: Condvar,
}

impl InputStream for IoStream {
    fn read_byte(&self) -> Result<u8, StreamError> {
        let mut stream_buf = self.data.lock().unwrap();
        while stream_buf.data.is_empty() {
            if stream_buf.eof_written {
                return Err(StreamError::ReadAfterEof);
            } else {
                stream_buf = self.cv.wait(stream_buf).unwrap();
            }
        }
        Ok(stream_buf.data.pop_front().unwrap())
    }

    fn read(&self, length: Option<usize>) -> Result<Vec<u8>, StreamError> {
        let mut stream_buf = self.data.lock().unwrap();
        if stream_buf.is_eof() {
            return Err(StreamError::ReadAfterEof);
        }
        let read_len = match length {
            Some(len) => {
                while stream_buf.data.len() < len {
                    if stream_buf.eof_written {
                        return Err(StreamError::UnexpectedEof);
                    } else {
                        stream_buf = self.cv.wait(stream_buf).unwrap();
                    }
                }
                len
            }
            None => stream_buf.data.len(),
        };
        let mut result = Vec::with_capacity(read_len);
        {
            let data_slices = stream_buf.data.as_slices();
            if read_len <= data_slices.0.len() {
                result.extend(&data_slices.0[..read_len]);
            } else {
                result.extend(data_slices.0);
                result.extend(&data_slices.1[..(read_len - data_slices.0.len())]);
            }
        }
        if read_len == stream_buf.data.len() {
            stream_buf.data.clear()
        } else {
            for _ in 0..read_len {
                stream_buf.data.pop_front();
            }
        }
        Ok(result)
    }

    fn is_empty(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.data.is_empty()
    }

    fn len(&self) -> usize {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.data.len()
    }

    fn eof_written(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.eof_written
    }

    fn is_eof(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.is_eof()
    }
}

impl OutputStream for IoStream {
    fn write_byte(&self, byte: u8) -> Result<(), StreamError> {
        let mut stream_buf = self.data.lock().unwrap();
        if stream_buf.eof_written {
            Err(StreamError::WriteAfterEof)
        } else {
            stream_buf.data.push_back(byte);
            self.cv.notify_one();
            Ok(())
        }
    }

    fn write(&self, buf: &[u8]) -> Result<usize, StreamError> {
        let mut stream_buf = self.data.lock().unwrap();
        if stream_buf.eof_written {
            Err(StreamError::WriteAfterEof)
        } else {
            stream_buf.data.extend(buf.iter());
            self.cv.notify_one();
            Ok(buf.len())
        }
    }

    fn write_eof(&self) {
        let mut stream_buf = self.data.lock().unwrap();
        stream_buf.eof_written = true;
        self.cv.notify_one();
    }
}
