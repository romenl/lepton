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

pub fn iostream(pre_load_len: usize) -> (InputStream, OutputStream) {
    let iostream = Arc::new(IoStream::default());
    (
        InputStream::new(iostream.clone(), pre_load_len),
        OutputStream { ostream: iostream },
    )
}

pub struct InputStream {
    istream: Arc<IoStream>,
    pre_load_buffer: Buffer,
    retained_buffer: Vec<u8>,
    processed_len: usize,
}

impl InputStream {
    pub fn new(istream: Arc<IoStream>, pre_load_len: usize) -> Self {
        InputStream {
            istream,
            pre_load_buffer: Buffer::new(pre_load_len),
            retained_buffer: vec![],
            processed_len: 0,
        }
    }

    #[inline(always)]
    pub fn peek_byte(&mut self) -> InputResult<u8> {
        let mut byte = [0u8];
        self.peek(&mut byte, true)?;
        Ok(byte[0])
    }

    #[inline(always)]
    pub fn read_byte(&mut self, keep: bool) -> InputResult<u8> {
        let mut byte = [0u8];
        self.read(&mut byte, true, keep)?;
        Ok(byte[0])
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
        self.read_internal(buf, fill, false, true)
    }

    pub fn read(&mut self, buf: &mut [u8], fill: bool, keep: bool) -> InputResult<usize> {
        self.read_internal(buf, fill, keep, true)
    }

    pub fn consume(&mut self, len: usize, keep: bool) -> InputResult<usize> {
        self.size_check(len);
        let preload_available = self.pre_load_buffer.data_len();
        let pre_loaded_len = min(preload_available, len);
        let (old_pre_load_start, old_pre_load_end) = (
            self.pre_load_buffer.read_offset,
            self.pre_load_buffer.write_offset,
        );
        let old_retained_len = self.retained_buffer.len();
        if keep {
            self.retained_buffer.resize(old_retained_len + len, 0);
            if preload_available > 0 {
                self.retained_buffer
                    .extend(self.pre_load_buffer.data_slice(
                        if pre_loaded_len == preload_available {
                            None
                        } else {
                            Some(pre_loaded_len)
                        },
                    ));
            }
        }
        self.pre_load_buffer.consume(pre_loaded_len);
        if len > preload_available {
            let extra_consume_len = len - preload_available;
            match self.istream
                .read(self.pre_load_buffer.slice_mut(), extra_consume_len)
            {
                Ok(len) => {
                    if keep {
                        self.retained_buffer
                            .extend(&self.pre_load_buffer.slice()[..extra_consume_len]);
                    }
                    self.pre_load_buffer.write_offset = len;
                    self.pre_load_buffer.read_offset = extra_consume_len;
                }
                Err(e) => {
                    if keep {
                        self.pre_load_buffer.write_offset = old_pre_load_end;
                        self.pre_load_buffer.read_offset = old_pre_load_start;
                        self.retained_buffer.truncate(old_retained_len);
                    }
                    return Err(e);
                }
            }
        }
        self.processed_len += len;
        Ok(len)
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
        &self.retained_buffer
    }

    #[inline(always)]
    pub fn clear_retained_data(&mut self) {
        self.retained_buffer.clear();
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

    fn read_internal(
        &mut self,
        buf: &mut [u8],
        fill: bool,
        keep: bool,
        consume: bool,
    ) -> InputResult<usize> {
        if buf.len() == 0 {
            return Ok(0);
        }
        if fill {
            self.size_check(buf.len());
        }
        let preload_available = self.pre_load_buffer.data_len();
        let mut read_len = 0;
        if preload_available > 0 {
            read_len = min(buf.len(), preload_available);
            buf[..read_len].copy_from_slice(self.pre_load_buffer.data_slice(Some(read_len)));
            self.pre_load_buffer.consume(read_len);
        }
        if preload_available == 0 || (fill && preload_available < buf.len()) {
            let buf_available = buf.len() - preload_available;
            let total_read_len = self.istream.read_internal(
                self.pre_load_buffer.slice_mut(),
                if fill { buf_available } else { 1 },
                consume,
            )?;
            self.pre_load_buffer.write_offset = total_read_len;
            self.pre_load_buffer.read_offset = 0;
            let size_to_fill = min(buf_available, total_read_len);
            buf[preload_available..(preload_available + size_to_fill)]
                .copy_from_slice(&self.pre_load_buffer.slice()[..size_to_fill]);
            self.pre_load_buffer.write_offset = total_read_len;
            self.pre_load_buffer.read_offset = size_to_fill;
            read_len += size_to_fill;
        }
        self.processed_len += read_len;
        if keep {
            self.retained_buffer.extend(&buf[..read_len]);
        }
        Ok(read_len)
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

    fn size_check(&self, len: usize) {
        if len > self.pre_load_buffer.capacity() {
            panic!(
                "needed buffer size {} greater than pre-load buffer capacity {}",
                len,
                self.pre_load_buffer.capacity()
            );
        }
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
        self.ostream.take_unread_data()
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
    pub fn peek(&self, buf: &mut [u8], min_len: usize) -> InputResult<usize> {
        self.read_internal(buf, min_len, false)
    }

    pub fn read(&self, buf: &mut [u8], min_len: usize) -> InputResult<usize> {
        self.read_internal(buf, min_len, true)
    }

    pub fn consume(&self, len: usize) -> InputResult<usize> {
        let mut stream_buf = self.lock_for_read()?;
        stream_buf = Self::wait_for_read(stream_buf, len, len, &self.cv)?;
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

    fn read_internal(&self, buf: &mut [u8], min_len: usize, consume: bool) -> InputResult<usize> {
        if min_len > buf.len() {
            panic!(
                "required minimum length {} is greater than buffer size {}",
                min_len,
                buf.len()
            );
        }
        let mut stream_buf = self.lock_for_read()?;
        stream_buf = Self::wait_for_read(stream_buf, min_len, buf.len(), &self.cv)?;
        let read_len = stream_buf.read(buf);
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
        min_len: usize,
        target_len: usize,
        cv: &Condvar,
    ) -> InputResult<MutexGuard<'a, StreamBuffer>> {
        while stream_buf.data.len() < min_len {
            stream_buf.validate_for_read(true)?;
            stream_buf.target_len = target_len;
            stream_buf = cv.wait(stream_buf).unwrap();
            stream_buf.target_len = 0;
        }
        Ok(stream_buf)
    }
}

// Methods for OutputStream
impl IoStream {
    pub fn write(&self, buf: &[u8]) -> OutputResult<usize> {
        let mut stream_buf = self.lock_for_write()?;
        stream_buf.data.extend(buf.iter());
        if stream_buf.data.len() >= stream_buf.target_len {
            self.cv.notify_one();
        }
        Ok(buf.len())
    }

    pub fn write_eof(&self) -> OutputResult<()> {
        let mut stream_buf = self.lock_for_write()?;
        stream_buf.eof_written = true;
        self.cv.notify_one();
        Ok(())
    }

    pub fn wait_for_close(&self) -> Result<(), ()> {
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

    pub fn take_unread_data(&self) -> Option<Vec<u8>> {
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
    target_len: usize,
    eof_written: bool,
    aborted: bool,
    reader_closed: bool,
}

impl StreamBuffer {
    fn read(&self, buf: &mut [u8]) -> usize {
        let read_len = min(buf.len(), self.data.len());
        let data_slices = self.data.as_slices();
        let first_slice_len = data_slices.0.len();
        if read_len <= first_slice_len {
            buf.clone_from_slice(&data_slices.0[..read_len]);
        } else {
            buf[..first_slice_len].clone_from_slice(data_slices.0);
            buf[first_slice_len..read_len]
                .clone_from_slice(&data_slices.1[..(read_len - first_slice_len)]);
        }
        read_len
    }

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

struct Buffer {
    data: Vec<u8>,
    write_offset: usize,
    read_offset: usize,
}

impl Buffer {
    fn new(size: usize) -> Self {
        Buffer {
            data: vec![0u8; size],
            write_offset: 0,
            read_offset: 0,
        }
    }

    fn data_len(&self) -> usize {
        self.write_offset - self.read_offset
    }

    fn capacity(&self) -> usize {
        self.data.len()
    }

    fn clear(&mut self) {
        self.write_offset = 0;
        self.read_offset = 0;
    }

    fn data_slice(&self, len: Option<usize>) -> &[u8] {
        match len {
            Some(len) => {
                let data_available = self.write_offset - self.read_offset;
                if len > data_available {
                    panic!(
                        "queried length {} greater than data available {}",
                        len, data_available
                    );
                }
                &self.data[self.read_offset..(self.read_offset + len)]
            }
            None => &self.data[self.read_offset..self.write_offset],
        }
    }

    fn slice(&self) -> &[u8] {
        &self.data
    }

    fn slice_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    fn commit_write(&mut self, len: usize) {
        let space_available = self.data.len() - self.write_offset;
        if len > space_available {
            panic!(
                "commit write length {} greater than space available {}",
                len, space_available
            );
        }
        self.write_offset += len;
    }

    fn consume(&mut self, len: usize) {
        let data_available = self.write_offset - self.read_offset;
        if len > data_available {
            panic!(
                "consume length {} greater than data available {}",
                len, data_available
            );
        }
        if len == data_available {
            self.write_offset = 0;
            self.read_offset = 0;
        } else {
            self.read_offset += len;
        }
    }
}

impl<'a, T> From<(InputError, MutexGuard<'a, T>)> for InputError {
    fn from(result: (InputError, MutexGuard<T>)) -> Self {
        result.0
    }
}
