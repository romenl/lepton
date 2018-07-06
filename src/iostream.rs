use std::cmp::min;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use byte_converter::ByteConverter;

pub type InputResult<T> = Result<T, InputError>;
pub type OutputResult<T> = Result<T, OutputError>;

const CONVERTER_BUF_LEN: usize = 8;

#[derive(Debug)]
pub enum InputError {
    StreamClosed,
    UnexpectedSigAbort(usize),
    UnexpectedEof(usize),
}

#[derive(Debug)]
pub enum OutputError {
    ReaderAborted,
    EofWritten,
}

pub fn iostream() -> (InputStream, OutputStream) {
    let iostream = Arc::new(IoStream::default());
    (
        InputStream::new(iostream.clone()),
        OutputStream { ostream: iostream },
    )
}

pub struct InputStream {
    istream: Arc<IoStream>,
    buffer: Vec<u8>,
    processed_len: usize,
}

impl InputStream {
    pub fn new(istream: Arc<IoStream>) -> Self {
        InputStream {
            istream,
            buffer: vec![],
            processed_len: 0,
        }
    }

    #[inline(always)]
    pub fn peek_byte(&self) -> InputResult<u8> {
        self.istream.peek_byte()
    }

    #[inline(always)]
    pub fn read_byte(&mut self, keep: bool) -> InputResult<u8> {
        let byte = self.istream.read_byte()?;
        self.processed_len += 1;
        if keep {
            self.buffer.push(byte);
        }
        Ok(byte)
    }

    #[inline(always)]
    pub fn read_u16<Converter: ByteConverter>(&mut self, keep: bool) -> InputResult<u16> {
        self.read_as_type(2, &Converter::slice_to_u16, keep)
    }

    #[inline(always)]
    pub fn read_u32<Converter: ByteConverter>(&mut self, keep: bool) -> InputResult<u32> {
        self.read_as_type(4, &Converter::slice_to_u32, keep)
    }

    #[inline(always)]
    pub fn peek(&mut self, buf: &mut [u8], fill: bool) -> InputResult<usize> {
        self.istream.peek(buf, fill)
    }

    #[inline(always)]
    pub fn read(&mut self, buf: &mut [u8], fill: bool, keep: bool) -> InputResult<usize> {
        let len = self.istream.read(buf, fill)?;
        self.processed_len += len;
        if keep {
            self.buffer.extend(&buf[..len]);
        }
        Ok(len)
    }

    #[inline(always)]
    pub fn consume(&mut self, len: usize, keep: bool) -> InputResult<usize> {
        if keep {
            let buffer_len = self.buffer.len();
            self.buffer.resize(buffer_len + len, 0);
            let result = self.istream.read(&mut self.buffer[buffer_len..], true);
            match result {
                Ok(len) => self.processed_len += len,
                Err(_) => self.buffer.truncate(buffer_len),
            }
            result
        } else {
            let len = self.istream.consume(len)?;
            self.processed_len += len;
            Ok(len)
        }
    }

    #[inline(always)]
    pub fn processed_len(&self) -> usize {
        self.processed_len
    }

    #[inline(always)]
    pub fn reset_processed_len(&mut self) {
        self.processed_len = 0;
    }

    #[inline(always)]
    pub fn view_retained_data(&self) -> &[u8] {
        &self.buffer
    }

    #[inline(always)]
    pub fn clear_retained_data(&mut self) {
        self.buffer.clear();
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.istream.is_empty()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.istream.len()
    }

    #[inline(always)]
    pub fn eof_written(&self) -> bool {
        self.istream.eof_written()
    }

    #[inline(always)]
    pub fn is_eof(&self) -> bool {
        self.istream.is_eof()
    }

    #[inline(always)]
    pub fn is_aborted(&self) -> bool {
        self.istream.is_aborted()
    }

    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        self.istream.is_closed()
    }

    #[inline(always)]
    pub fn abort(&self) {
        self.istream.abort()
    }

    #[inline(always)]
    pub fn close(&self) {
        self.istream.close()
    }

    #[inline(always)]
    fn read_as_type<T>(
        &mut self,
        len: usize,
        converter: &Fn(&[u8]) -> T,
        keep: bool,
    ) -> InputResult<T> {
        assert!(len <= CONVERTER_BUF_LEN);
        let mut buf = [0u8; CONVERTER_BUF_LEN];
        self.read(&mut buf[..len], true, keep)?;
        Ok(converter(&buf[..len]))
    }
}

#[derive(Clone)]
pub struct OutputStream {
    ostream: Arc<IoStream>,
}

impl OutputStream {
    pub fn new(ostream: Arc<IoStream>) -> Self {
        OutputStream { ostream }
    }

    #[inline(always)]
    pub fn write_byte(&self, byte: u8) -> OutputResult<()> {
        self.ostream.write_byte(byte)
    }

    #[inline(always)]
    pub fn write(&self, buf: &[u8]) -> OutputResult<usize> {
        self.ostream.write(buf)
    }

    #[inline(always)]
    pub fn write_eof(&self) -> OutputResult<()> {
        self.ostream.write_eof()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.ostream.is_empty()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.ostream.len()
    }

    #[inline(always)]
    pub fn eof_written(&self) -> bool {
        self.ostream.eof_written()
    }

    #[inline(always)]
    pub fn is_aborted(&self) -> bool {
        self.ostream.is_aborted()
    }

    #[inline(always)]
    pub fn is_closed(&self) -> bool {
        self.ostream.is_closed()
    }

    #[inline(always)]
    pub fn wait_for_close(&self) -> Result<(), ()> {
        self.ostream.wait_for_close()
    }

    #[inline(always)]
    pub fn unread_data(&self) -> Option<Vec<u8>> {
        self.ostream.unread_data()
    }
}

#[derive(Default)]
pub struct IoStream {
    data: Mutex<StreamBuffer>,
    cv: Condvar,
}

// Methods shared by InputStream and OutputStream
impl IoStream {
    pub fn is_empty(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.data.is_empty()
    }

    pub fn len(&self) -> usize {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.data.len()
    }

    pub fn eof_written(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.eof_written
    }

    pub fn is_aborted(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.aborted
    }

    pub fn is_closed(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.reader_closed
    }
}

// Methods for InputStream
impl IoStream {
    pub fn peek_byte(&self) -> InputResult<u8> {
        self.read_byte_internal(false)
    }

    pub fn read_byte(&self) -> InputResult<u8> {
        self.read_byte_internal(true)
    }

    pub fn peek(&self, buf: &mut [u8], fill: bool) -> InputResult<usize> {
        self.read_internal(buf, fill, false)
    }

    pub fn read(&self, buf: &mut [u8], fill: bool) -> InputResult<usize> {
        self.read_internal(buf, fill, true)
    }

    pub fn consume(&self, len: usize) -> InputResult<usize> {
        let mut stream_buf = self.lock_for_read()?;
        stream_buf = Self::wait_for_read(stream_buf, len, &self.cv)?;
        Ok(stream_buf.consume(len))
    }

    pub fn is_eof(&self) -> bool {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.is_eof()
    }

    pub fn abort(&self) {
        let mut stream_buf = self.data.lock().unwrap();
        stream_buf.aborted = true;
    }

    pub fn close(&self) {
        let mut stream_buf = self.data.lock().unwrap();
        stream_buf.aborted = true;
        stream_buf.reader_closed = true;
        self.cv.notify_one();
    }

    fn read_byte_internal(&self, consume: bool) -> InputResult<u8> {
        let mut stream_buf = self.data.lock().unwrap();
        while stream_buf.data.is_empty() {
            stream_buf.validate_for_read(false)?;
            stream_buf = self.cv.wait(stream_buf).unwrap();
        }
        if consume {
            Ok(stream_buf.data.pop_front().unwrap())
        } else {
            Ok(stream_buf.data.front().unwrap().clone())
        }
    }

    fn read_internal(&self, buf: &mut [u8], fill: bool, consume: bool) -> InputResult<usize> {
        let mut stream_buf = self.lock_for_read()?;
        let read_len = match fill {
            true => {
                stream_buf = Self::wait_for_read(stream_buf, buf.len(), &self.cv)?;
                buf.len()
            }
            false => min(stream_buf.data.len(), buf.len()),
        };
        {
            let data_slices = stream_buf.data.as_slices();
            let first_slice_len = data_slices.0.len();
            if read_len <= first_slice_len {
                buf.clone_from_slice(&data_slices.0[..read_len]);
            } else {
                buf[..first_slice_len].clone_from_slice(data_slices.0);
                buf[first_slice_len..read_len]
                    .clone_from_slice(&data_slices.1[..(read_len - first_slice_len)]);
            }
        }
        if consume {
            stream_buf.consume(read_len);
        }
        Ok(read_len)
    }

    fn lock_for_read(&self) -> InputResult<MutexGuard<StreamBuffer>> {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.validate_for_read(false)?;
        Ok(stream_buf)
    }

    fn wait_for_read<'a>(
        mut stream_buf: MutexGuard<'a, StreamBuffer>,
        len: usize,
        cv: &Condvar,
    ) -> InputResult<MutexGuard<'a, StreamBuffer>> {
        while stream_buf.data.len() < len {
            stream_buf.validate_for_read(true)?;
            stream_buf = cv.wait(stream_buf).unwrap();
        }
        Ok(stream_buf)
    }
}

// Methods for OutputStream
impl IoStream {
    pub fn write_byte(&self, byte: u8) -> OutputResult<()> {
        let mut stream_buf = self.lock_for_write()?;
        stream_buf.data.push_back(byte);
        self.cv.notify_one();
        Ok(())
    }

    pub fn write(&self, buf: &[u8]) -> OutputResult<usize> {
        let mut stream_buf = self.lock_for_write()?;
        stream_buf.data.extend(buf.iter());
        self.cv.notify_one();
        Ok(buf.len())
    }

    pub fn write_eof(&self) -> OutputResult<()> {
        let mut stream_buf = self.lock_for_write()?;
        stream_buf.eof_written = true;
        self.cv.notify_one();
        Ok(())
    }

    pub fn wait_for_close(&self) -> Result <(), ()> {
        let mut stream_buf = self.data.lock().unwrap();
        if !stream_buf.aborted {
            Err(())
        } else {
            while !stream_buf.reader_closed {
                stream_buf = self.cv.wait(stream_buf).unwrap();
            }
            Ok(())
        }
    }

    pub fn unread_data(&self) -> Option<Vec<u8>> {
        let mut stream_buf = self.data.lock().unwrap();
        if stream_buf.reader_closed && !stream_buf.data.is_empty() {
            Some(stream_buf.data.drain(..).collect())
        } else {
            None
        }
    }

    fn lock_for_write(&self) -> OutputResult<MutexGuard<StreamBuffer>> {
        let stream_buf = self.data.lock().unwrap();
        stream_buf.validate_for_write()?;
        Ok(stream_buf)
    }
}

#[derive(Default)]
struct StreamBuffer {
    data: VecDeque<u8>,
    eof_written: bool,
    aborted: bool,
    reader_closed: bool,
}

impl StreamBuffer {
    fn consume(&mut self, len: usize) -> usize {
        if len < self.data.len() {
            for _ in 0..len {
                self.data.pop_front();
            }
            len
        } else {
            let ret = self.data.len();
            self.data.clear();
            ret
        }
    }

    fn validate_for_read(&self, require_active: bool) -> InputResult<()> {
        use self::InputError::*;
        if self.reader_closed {
            return Err(StreamClosed);
        }
        if self.data.is_empty() || require_active {
            if self.aborted {
                return Err(UnexpectedSigAbort(self.data.len()));
            } else if self.eof_written {
                return Err(UnexpectedEof(self.data.len()));
            }
        }
        Ok(())
    }

    fn validate_for_write(&self) -> OutputResult<()> {
        use self::OutputError::*;
        if self.aborted {
            Err(ReaderAborted)
        } else if self.eof_written {
            Err(EofWritten)
        } else {
            Ok(())
        }
    }

    fn is_eof(&self) -> bool {
        self.data.is_empty() && self.eof_written
    }
}
