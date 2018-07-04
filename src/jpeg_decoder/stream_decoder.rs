use std::thread;

use super::decoder::JpegDecoder;
use super::error::JpegResult;
use iostream::{iostream, OutputStream};

pub struct JpegStreamDecoder {
    decoder_handle: Option<thread::JoinHandle<JpegResult<()>>>,
    ostream: OutputStream,
}

impl JpegStreamDecoder {
    pub fn new() -> JpegResult<Self> {
        let (istream, ostream) = iostream();
        let decoder_handle = thread::Builder::new()
            .name("decoder thread".to_owned())
            .spawn(move || JpegDecoder::new(istream).decode())?;
        Ok(JpegStreamDecoder {
            decoder_handle: Some(decoder_handle),
            ostream,
        })
    }

    pub fn decode(&mut self, input: &[u8], input_offset: &mut usize) -> JpegResult<()> {
        match self.ostream.write(&input[*input_offset..]) {
            Ok(_) => {
                *input_offset = input.len();
                Ok(())
            }
            Err(_) => self.finish(),
        }
    }

    pub fn flush(&mut self) -> JpegResult<()> {
        let _ = self.ostream.write_eof();
        self.finish()
    }

    fn finish(&mut self) -> JpegResult<()> {
        // TODO: Handle the case where docoder_handle is already taken
        match self.decoder_handle.take().unwrap().join().unwrap() {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
}
