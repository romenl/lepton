use std::cmp::max;
use std::mem::replace;
use std::ops::Range;

use super::error::{JpegError, JpegResult, UnsupportedFeature};
use super::huffman::{fill_default_mjpeg_tables, HuffmanDecoder, HuffmanTable};
use super::jpeg::{
    CodingProcess, Component, EntropyCoding, FormatInfo, FrameInfo, Jpeg, Scan, ScanTruncation,
};
use super::marker::Marker;
use super::parser::{parse_app, parse_com, parse_dht, parse_dqt, parse_dri, parse_sof, parse_sos};
use iostream::InputStream;
use thread_handoff::ThreadHandoffExt;

pub type DecodeResult = JpegResult<Jpeg>;

pub const MAX_COMPONENTS: usize = 4;

static UNZIGZAG: [usize; 64] = [
    0, 1, 8, 16, 9, 2, 3, 10, 17, 24, 32, 25, 18, 11, 4, 5, 12, 19, 26, 33, 40, 48, 41, 34, 27, 20,
    13, 6, 7, 14, 21, 28, 35, 42, 49, 56, 57, 50, 43, 36, 29, 22, 15, 23, 30, 37, 44, 51, 58, 59,
    52, 45, 38, 31, 39, 46, 53, 60, 61, 54, 47, 55, 62, 63,
];

/// JPEG decoder
pub struct JpegDecoder {
    input: InputStream,
    dc_huffman_tables: [Option<HuffmanTable>; 4],
    ac_huffman_tables: [Option<HuffmanTable>; 4],
    quantization_tables: [Option<[u16; 64]>; 4],
    restart_interval: u16,
    is_mjpeg: bool,
    n_scan_processed: usize,
    coefficients: Vec<Vec<i16>>,
    header_only: bool,
    start_byte: usize,
}

impl JpegDecoder {
    pub fn new(input: InputStream, start_byte: usize, header_only: bool) -> Self {
        JpegDecoder {
            input: input,
            dc_huffman_tables: [None, None, None, None],
            ac_huffman_tables: [None, None, None, None],
            quantization_tables: [None, None, None, None],
            restart_interval: 0,
            is_mjpeg: false,
            n_scan_processed: 0,
            coefficients: Vec::new(),
            header_only: header_only,
            start_byte: if header_only { 0 } else { start_byte },
        }
    }

    pub fn decode(mut self) -> DecodeResult {
        let mut frame = None;
        let mut scans = vec![];
        let mut format = if self.header_only {
            None
        } else {
            Some(FormatInfo::default())
        };
        match self.decode_internal(&mut frame, &mut scans, &mut format) {
            Ok(_) => (),
            Err(JpegError::EOF) => match format {
                Some(ref format) => {
                    if self.input.processed_len() < self.start_byte {
                        return Err(JpegError::Malformatted(
                            "EOF encountered before start_byte".to_owned(),
                        ));
                    }
                    if format.handoff.is_empty() {
                        return Err(JpegError::Malformatted(
                            "no/insufficient entropy encoded data".to_owned(),
                        ));
                    }
                    if scans.last().unwrap().is_empty() {
                        scans.pop();
                    }
                }
                None => return Err(JpegError::Malformatted("incomplete header".to_owned())),
            },
            Err(e) => return Err(e),
        }
        self.input.abort();
        if let Some(ref mut format) = format {
            format.grb.extend(self.input.view_retained_data());
            let old_grb_len = format.grb.len();
            format.grb.resize(old_grb_len + self.input.len(), 0);
            self.input
                .read(&mut format.grb[old_grb_len..], true, false)
                .unwrap();
        }
        self.input.close();
        Ok(Jpeg {
            frame: frame.unwrap(),
            scans,
            format,
        })
    }

    fn decode_internal(
        &mut self,
        frame: &mut Option<FrameInfo>,
        scans: &mut Vec<Scan>,
        format: &mut Option<FormatInfo>,
    ) -> JpegResult<()> {
        if self.input.read_byte(true)? != 0xFF
            || Marker::from_u8(try!(self.input.read_byte(true))) != Ok(Marker::SOI)
        {
            return Err(JpegError::Malformatted(
                "first two bytes is not a SOI marker".to_owned(),
            ));
        }
        let mut previous_marker = Marker::SOI;
        loop {
            let marker = self.read_marker()?;
            match marker {
                // Frame header
                Marker::SOF(n) => {
                    // Section 4.10
                    // "An image contains only one frame in the cases of sequential and
                    //  progressive coding processes; an image contains multiple frames for the
                    //  hierarchical mode."
                    if frame.is_some() {
                        return Err(JpegError::Unsupported(UnsupportedFeature::Hierarchical));
                    }
                    let frame_info = parse_sof(&mut self.input, n)?;
                    let component_count = frame_info.components.len();

                    if frame_info.is_differential {
                        return Err(JpegError::Unsupported(UnsupportedFeature::Hierarchical));
                    }
                    if frame_info.coding_process == CodingProcess::Lossless {
                        return Err(JpegError::Unsupported(UnsupportedFeature::Lossless));
                    }
                    if frame_info.entropy_coding == EntropyCoding::Arithmetic {
                        return Err(JpegError::Unsupported(
                            UnsupportedFeature::ArithmeticEntropyCoding,
                        ));
                    }
                    // TODO: Do we support higher precision?
                    if frame_info.precision != 8 {
                        return Err(JpegError::Unsupported(UnsupportedFeature::SamplePrecision(
                            frame_info.precision,
                        )));
                    }
                    if frame_info.size.height == 0 {
                        return Err(JpegError::Unsupported(UnsupportedFeature::DNL));
                    }
                    if component_count != 1 && component_count != 3 && component_count != 4 {
                        return Err(JpegError::Unsupported(UnsupportedFeature::ComponentCount(
                            component_count as u8,
                        )));
                    }
                    replace(frame, Some(frame_info));
                }
                // Scan header
                Marker::SOS => {
                    if frame.is_none() {
                        return Err(JpegError::Malformatted(
                            "scan encountered before frame".to_owned(),
                        ));
                    }
                    let frame_info = frame.as_ref().unwrap();
                    let scan_info = parse_sos(&mut self.input, frame_info)?;
                    scans.push(Scan::new(self.input.view_retained_data(), scan_info));
                    if self.in_pge() {
                        format.as_mut().unwrap().pge.extend(
                            &self.input.view_retained_data()[max(
                                self.input.view_retained_data().len() - self.input.processed_len()
                                    + self.start_byte,
                                0,
                            )..],
                        );
                    }
                    self.input.clear_retained_data();
                    // TODO: Replace this with coefficients in Scan
                    if self.coefficients.is_empty() {
                        self.coefficients = frame_info
                            .components
                            .iter()
                            .map(|c| {
                                let block_count = c.size_in_block.width as usize
                                    * c.size_in_block.height as usize;
                                vec![0; block_count * 64]
                            })
                            .collect();
                    }
                    if let Some(ref mut format) = format {
                        let current_scan = scans.last_mut().unwrap();
                        self.decode_scan(frame_info, current_scan, format)?;
                        if self.start_byte > 0 {
                            current_scan.coefficients = None;
                        }
                    }
                    self.n_scan_processed += 1;
                }
                // Table-specification and miscellaneous markers
                // Quantization table-specification
                Marker::DQT => {
                    parse_dqt(&mut self.input, &mut self.quantization_tables)?;
                }
                // Huffman table-specification
                Marker::DHT => {
                    let is_baseline = frame.as_ref().map(|frame| frame.is_baseline);
                    /* let (dc_tables, ac_tables) = */
                    parse_dht(
                        &mut self.input,
                        is_baseline,
                        &mut self.dc_huffman_tables,
                        &mut self.ac_huffman_tables,
                    )?;
                }
                // Arithmetic conditioning table-specification
                Marker::DAC => {
                    return Err(JpegError::Unsupported(
                        UnsupportedFeature::ArithmeticEntropyCoding,
                    ))
                }
                // Restart interval definition
                Marker::DRI => self.restart_interval = parse_dri(&mut self.input)?,
                // Comment
                Marker::COM => {
                    let _comment = parse_com(&mut self.input)?;
                }
                // Application data
                Marker::APP(_) => {
                    self.is_mjpeg = parse_app(&mut self.input, marker)?;
                }
                // Restart
                Marker::RST(_) => {
                    // Some encoders emit a final RST marker after entropy-coded data, which
                    // decode_scan does not take care of. So if we encounter one, we ignore it.
                    if previous_marker != Marker::SOS {
                        return Err(JpegError::Malformatted(
                            "RST found outside of entropy-coded data".to_owned(),
                        ));
                    }
                }
                // Define number of lines
                Marker::DNL => {
                    // Section B.2.1
                    // "If a DNL segment (see B.2.5) is present, it shall immediately follow the first scan."
                    if previous_marker != Marker::SOS || self.n_scan_processed != 1 {
                        return Err(JpegError::Malformatted(
                            "DNL is only allowed immediately after the first scan".to_owned(),
                        ));
                    }
                    return Err(JpegError::Unsupported(UnsupportedFeature::DNL));
                }
                // Hierarchical mode markers
                Marker::DHP | Marker::EXP => {
                    return Err(JpegError::Unsupported(UnsupportedFeature::Hierarchical))
                }
                // End of image
                Marker::EOI => break,
                _ => {
                    return Err(JpegError::Malformatted(format!(
                        "{:?} marker found where not allowed",
                        marker
                    )))
                }
            }
            previous_marker = marker;
        }
        Ok(())
    }

    fn read_marker(&mut self) -> JpegResult<Marker> {
        // This should be an error as the JPEG spec doesn't allow extraneous data between marker segments.
        // libjpeg allows this though and there are images in the wild utilising it, so we are
        // forced to support this behavior.
        // Sony Ericsson P990i is an example of a device which produce this sort of JPEGs.
        while self.input.read_byte(true)? != 0xFF {}
        let mut byte = self.input.read_byte(true)?;
        // Section B.1.1.2
        // "Any marker may optionally be preceded by any number of fill bytes, which are bytes assigned code X’FF’."
        while byte == 0xFF {
            byte = self.input.read_byte(true)?;
        }
        match byte {
            0x00 => Err(JpegError::Malformatted(
                "0xFF00 found where marker was expected (read_marker)".to_owned(),
            )),
            _ => Ok(Marker::from_u8(byte).unwrap()),
        }
    }

    fn decode_scan(
        &mut self,
        frame: &FrameInfo,
        scan: &mut Scan,
        format: &mut FormatInfo,
    ) -> JpegResult<()> {
        let components: Vec<Component>;
        {
            let scan_info = &scan.info;
            assert!(scan_info.component_indices.len() <= MAX_COMPONENTS);
            // TODO: scan.coefficients = Some()
            components = scan_info
                .component_indices
                .iter()
                .map(|&i| frame.components[i].clone())
                .collect();
            // Verify that all required quantization tables has been set.
            if components.iter().any(|component| {
                self.quantization_tables[component.quantization_table_index].is_none()
            }) {
                return Err(JpegError::Malformatted(
                    "use of unset quantization table".to_owned(),
                ));
            }
            if self.is_mjpeg {
                fill_default_mjpeg_tables(
                    scan_info,
                    &mut self.dc_huffman_tables,
                    &mut self.ac_huffman_tables,
                );
            }
            // Verify that all required huffman tables has been set.
            if scan_info.spectral_selection.start == 0
                && scan_info
                    .dc_table_indices
                    .iter()
                    .any(|&i| self.dc_huffman_tables[i].is_none())
            {
                return Err(JpegError::Malformatted(
                    "scan makes use of unset dc huffman table".to_owned(),
                ));
            }
            if scan_info.spectral_selection.end > 1
                && scan_info
                    .ac_table_indices
                    .iter()
                    .any(|&i| self.ac_huffman_tables[i].is_none())
            {
                return Err(JpegError::Malformatted(
                    "scan makes use of unset ac huffman table".to_owned(),
                ));
            }
        }
        let is_interleaved = components.len() > 1;
        let mut huffman = HuffmanDecoder::new(self.start_byte);
        let mut dc_predictors = [0i16; MAX_COMPONENTS];
        let mut n_mcu_left_until_restart = self.restart_interval;
        let mut expected_rst_num: u8 = 0;
        let mut eob_run: u16 = 0;
        let &size_in_mcu = if is_interleaved {
            &frame.size_in_mcu
        } else {
            &components[0].size_in_block
        };
        for mcu_y in 0..size_in_mcu.height as usize {
            if self.in_pge() {
                self.start_byte = 0;
                huffman.end_pge();
                format.pge.extend(huffman.get_pge());
            }
            if self.start_byte == 0 {
                let (overhang_byte, n_overhang_bit) = huffman.handover_byte();
                format.handoff.push(ThreadHandoffExt {
                    start_scan: self.n_scan_processed as u16,
                    end_scan: self.n_scan_processed as u16,
                    mcu_y_start: mcu_y as u16,
                    segment_size: self.input.processed_len() as u32,
                    overhang_byte,
                    n_overhang_bit,
                    last_dc: [0u16; 4], // FIXME: Fill last_dc with correct value
                })
            }
            for mcu_x in 0..size_in_mcu.width as usize {
                if is_interleaved {
                    for (i, component) in components.iter().enumerate() {
                        for block_y_offset in 0..component.vertical_sampling_factor as usize {
                            for block_x_offset in 0..component.horizontal_sampling_factor as usize {
                                let block_y = mcu_y * component.vertical_sampling_factor as usize
                                    + block_y_offset;
                                let block_x = mcu_x * component.horizontal_sampling_factor as usize
                                    + block_x_offset;
                                self.decode_block(
                                    block_y,
                                    block_x,
                                    i,
                                    component,
                                    scan,
                                    &mut huffman,
                                    &mut eob_run,
                                    &mut dc_predictors[i],
                                    format,
                                )?;
                            }
                        }
                    }
                } else {
                    self.decode_block(
                        mcu_y,
                        mcu_x,
                        0,
                        &components[0],
                        scan,
                        &mut huffman,
                        &mut eob_run,
                        &mut dc_predictors[0],
                        format,
                    )?;
                }
                if self.restart_interval > 0 {
                    n_mcu_left_until_restart -= 1;
                    let is_last_mcu = mcu_x as u16 == frame.size_in_mcu.width - 1
                        && mcu_y as u16 == frame.size_in_mcu.height - 1;
                    if n_mcu_left_until_restart == 0 && !is_last_mcu {
                        // TODO: Check fill bits
                        huffman.clear_buffer();
                        match huffman.read_rst(&mut self.input, expected_rst_num) {
                            Ok(_) => {
                                huffman.reset();
                                // Section F.2.1.3.1
                                dc_predictors = [0i16; MAX_COMPONENTS];
                                // Section G.1.2.2
                                eob_run = 0;
                                expected_rst_num = (expected_rst_num + 1) % 8;
                                n_mcu_left_until_restart = self.restart_interval;
                            }
                            Err(JpegError::EOF) => {
                                format.grb.extend(huffman.view_buffer());
                                return Err(JpegError::EOF);
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
            }
        }
        if self.in_pge() {
            format.pge.extend(huffman.get_pge());
        }
        Ok(())
    }

    fn decode_block(
        &mut self,
        y: usize,
        x: usize,
        scan_component_index: usize,
        component: &Component,
        scan: &mut Scan,
        huffman: &mut HuffmanDecoder,
        eob_run: &mut u16,
        dc_predictor: &mut i16,
        format: &mut FormatInfo,
    ) -> JpegResult<()> {
        let scan_info = &scan.info;
        let block_offset = (y * component.size_in_block.width as usize + x) * 64;
        let coefficients = &mut self.coefficients
            [scan_info.component_indices[scan_component_index]][block_offset..block_offset + 64];
        huffman.clear_buffer();
        match if scan_info.successive_approximation_high == 0 {
            decode_block(
                &mut self.input,
                coefficients,
                huffman,
                self.dc_huffman_tables[scan_info.dc_table_indices[scan_component_index]]
                    .as_ref()
                    .unwrap(),
                self.ac_huffman_tables[scan_info.ac_table_indices[scan_component_index]]
                    .as_ref()
                    .unwrap(),
                scan_info.spectral_selection.clone(),
                scan_info.successive_approximation_low,
                eob_run,
                dc_predictor,
            )
        } else {
            decode_block_successive_approximation(
                &mut self.input,
                coefficients,
                huffman,
                self.ac_huffman_tables[scan_info.ac_table_indices[scan_component_index]]
                    .as_ref()
                    .unwrap(),
                scan_info.spectral_selection.clone(),
                scan_info.successive_approximation_low,
                eob_run,
            )
        } {
            Ok(()) => Ok(()),
            Err(JpegError::EOF) => {
                if self.input.processed_len() < self.start_byte {
                    return Err(JpegError::Malformatted(
                        "EOF encountered before start_byte".to_owned(),
                    ));
                }
                if scan_component_index == 0 && x == 0 {
                    // Leftmost column of the first index of the scan
                    if y == 0 {
                        // First block of the scan
                        pop_handoff_and_verify_non_empty(format)?;
                        format.grb.extend(&scan.raw_header);
                    } else if scan_info.component_indices.len() == 1
                        || y % component.vertical_sampling_factor as usize == 0
                    {
                        // First block in the MCU row
                        pop_handoff_and_verify_non_empty(format)?;
                        format.grb.extend(huffman.view_buffer());
                    }
                }
                format.grb.extend(huffman.view_buffer());
                scan.truncation = Some(ScanTruncation::new(scan_component_index, y, x));
                Err(JpegError::EOF)
            }
            Err(e) => Err(e),
        }
    }

    fn in_pge(&self) -> bool {
        self.start_byte > 0 && self.input.processed_len() > self.start_byte
    }
}

fn pop_handoff_and_verify_non_empty(format: &mut FormatInfo) -> JpegResult<()> {
    format.handoff.pop();
    if format.handoff.is_empty() {
        Err(JpegError::EOF)
    } else {
        Ok(())
    }
}

fn decode_block(
    input: &mut InputStream,
    coefficients: &mut [i16],
    huffman: &mut HuffmanDecoder,
    dc_table: &HuffmanTable,
    ac_table: &HuffmanTable,
    spectral_selection: Range<u8>,
    successive_approximation_low: u8,
    eob_run: &mut u16,
    dc_predictor: &mut i16,
) -> JpegResult<()> {
    debug_assert_eq!(coefficients.len(), 64);
    if spectral_selection.start == 0 {
        // Section F.2.2.1
        // Figure F.12
        let value = huffman.decode(input, dc_table)?;
        let diff = match value {
            0 => 0,
            _ => {
                // Section F.1.2.1.1
                // Table F.1
                if value > 11 {
                    return Err(JpegError::Malformatted(
                        "invalid DC difference magnitude category".to_owned(),
                    ));
                }
                huffman.receive_extend(input, value)?
            }
        };
        // Malicious JPEG files can cause this add to overflow, therefore we use wrapping_add.
        // One example of such a file is tests/crashtest/images/dc-predictor-overflow.jpg
        *dc_predictor = dc_predictor.wrapping_add(diff);
        coefficients[0] = *dc_predictor << successive_approximation_low;
    }
    let mut index = max(spectral_selection.start, 1);
    if *eob_run > 0 {
        *eob_run -= 1;
        return Ok(());
    }
    // Section F.1.2.2.1
    while index < spectral_selection.end {
        if let Some((value, run)) = huffman.decode_fast_ac(input, ac_table)? {
            index += run;
            if index >= spectral_selection.end {
                break;
            }
            coefficients[UNZIGZAG[index as usize]] = value << successive_approximation_low;
            index += 1;
        } else {
            let byte = huffman.decode(input, ac_table)?;
            let r = byte >> 4;
            let s = byte & 0x0f;
            if s == 0 {
                match r {
                    15 => index += 16, // Run length of 16 zero coefficients.
                    _ => {
                        *eob_run = (1 << r) - 1;
                        if r > 0 {
                            *eob_run += huffman.get_bits(input, r)?;
                        }
                        break;
                    }
                }
            } else {
                index += r;
                if index >= spectral_selection.end {
                    break;
                }
                coefficients[UNZIGZAG[index as usize]] =
                    huffman.receive_extend(input, s)? << successive_approximation_low;
                index += 1;
            }
        }
    }
    Ok(())
}

fn decode_block_successive_approximation(
    input: &mut InputStream,
    coefficients: &mut [i16],
    huffman: &mut HuffmanDecoder,
    ac_table: &HuffmanTable,
    spectral_selection: Range<u8>,
    successive_approximation_low: u8,
    eob_run: &mut u16,
) -> JpegResult<()> {
    // TODO: Can we simply this or its lepton encoding?
    debug_assert_eq!(coefficients.len(), 64);
    let bit = 1 << successive_approximation_low;
    if spectral_selection.start == 0 {
        // Section G.1.2.1
        if huffman.get_bits(input, 1)? == 1 {
            coefficients[0] |= bit;
        }
    } else {
        // Section G.1.2.3
        if *eob_run > 0 {
            *eob_run -= 1;
            refine_non_zeroes(input, coefficients, huffman, spectral_selection, 64, bit)?;
            return Ok(());
        }
        let mut index = spectral_selection.start;
        while index < spectral_selection.end {
            let byte = huffman.decode(input, ac_table)?;
            let r = byte >> 4;
            let s = byte & 0x0f;
            let mut zero_run_length = r;
            let mut value = 0;
            match s {
                0 => {
                    match r {
                        15 => {
                            // Run length of 16 zero coefficients.
                            // We don't need to do anything special here, zero_run_length is 15
                            // and then value (which is zero) gets written, resulting in 16
                            // zero coefficients.
                        }
                        _ => {
                            *eob_run = (1 << r) - 1;
                            if r > 0 {
                                *eob_run += huffman.get_bits(input, r)?;
                            }
                            // Force end of block.
                            zero_run_length = 64;
                        }
                    }
                }
                1 => {
                    if huffman.get_bits(input, 1)? == 1 {
                        value = bit;
                    } else {
                        value = -bit;
                    }
                }
                _ => {
                    return Err(JpegError::Malformatted(
                        "unexpected huffman code".to_owned(),
                    ))
                }
            }
            let range = index..spectral_selection.end;
            index = refine_non_zeroes(input, coefficients, huffman, range, zero_run_length, bit)?;
            if value != 0 {
                coefficients[UNZIGZAG[index as usize]] = value;
            }
            index += 1;
        }
    }
    Ok(())
}

fn refine_non_zeroes(
    input: &mut InputStream,
    coefficients: &mut [i16],
    huffman: &mut HuffmanDecoder,
    range: Range<u8>,
    zrl: u8,
    bit: i16,
) -> JpegResult<u8> {
    debug_assert_eq!(coefficients.len(), 64);
    let last = range.end - 1;
    let mut zero_run_length = zrl;
    for i in range {
        let index = UNZIGZAG[i as usize];
        if coefficients[index] == 0 {
            if zero_run_length == 0 {
                return Ok(i);
            }
            zero_run_length -= 1;
        } else if huffman.get_bits(input, 1)? == 1 && coefficients[index] & bit == 0 {
            if coefficients[index] > 0 {
                coefficients[index] += bit;
            } else {
                coefficients[index] -= bit;
            }
        }
    }
    Ok(last)
}
