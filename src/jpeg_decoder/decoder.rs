use std::cmp;
use std::ops::Range;

use super::error::{JpegError, JpegResult, UnsupportedFeature};
use super::huffman::{fill_default_mjpeg_tables, HuffmanDecoder, HuffmanTable};
use super::marker::Marker;
use super::parser::{
    parse_app, parse_com, parse_dht, parse_dqt, parse_dri, parse_sof, parse_sos,
    AdobeColorTransform, AppData, CodingProcess, Component, EntropyCoding, FrameInfo,
    ScanInfo,
};
use iostream::InputStream;

pub const MAX_COMPONENTS: usize = 4;

static UNZIGZAG: [usize; 64] = [
    0, 1, 8, 16, 9, 2, 3, 10, 17, 24, 32, 25, 18, 11, 4, 5, 12, 19, 26, 33, 40, 48, 41, 34, 27, 20,
    13, 6, 7, 14, 21, 28, 35, 42, 49, 56, 57, 50, 43, 36, 29, 22, 15, 23, 30, 37, 44, 51, 58, 59,
    52, 45, 38, 31, 39, 46, 53, 60, 61, 54, 47, 55, 62, 63,
];

/// JPEG decoder
pub struct JpegDecoder {
    input: InputStream,

    frame: Option<FrameInfo>, // TODO: Make this a variable in the function?
    dc_huffman_tables: [Option<HuffmanTable>; 4],
    ac_huffman_tables: [Option<HuffmanTable>; 4],
    quantization_tables: [Option<[u16; 64]>; 4],

    restart_interval: u16,
    color_transform: Option<AdobeColorTransform>,
    is_jfif: bool,
    is_mjpeg: bool,

    // Used for progressive JPEGs.
    coefficients: Vec<Vec<i16>>,
    // Bitmask of which coefficients has been completely decoded.
    coefficient_finished: [u64; MAX_COMPONENTS],
}

impl JpegDecoder {
    /// Creates a new `Decoder` using the reader `reader`.
    pub fn new(input: InputStream) -> Self {
        JpegDecoder {
            input: input,
            frame: None,
            dc_huffman_tables: [None, None, None, None],
            ac_huffman_tables: [None, None, None, None],
            quantization_tables: [None, None, None, None],
            restart_interval: 0,
            color_transform: None,
            is_jfif: false,
            is_mjpeg: false,
            coefficients: Vec::new(),
            coefficient_finished: [0; MAX_COMPONENTS],
        }
    }

    /// Decodes the image and returns the decoded pixels if successful.
    pub fn decode(&mut self) -> JpegResult<()> {
        if self.input.read_byte(true)? != 0xFF
            || Marker::from_u8(try!(self.input.read_byte(true))) != Ok(Marker::SOI)
        {
            return Err(JpegError::Malformatted(
                "first two bytes is not a SOI marker".to_owned(),
            ));
        }
        let mut previous_marker = Marker::SOI;
        let mut pending_marker = None;
        let mut scans_processed = 0;
        loop {
            let marker = match pending_marker.take() {
                Some(m) => m,
                None => self.read_marker()?,
            };
            match marker {
                // Frame header
                Marker::SOF(n) => {
                    // Section 4.10
                    // "An image contains only one frame in the cases of sequential and
                    //  progressive coding processes; an image contains multiple frames for the
                    //  hierarchical mode."
                    if self.frame.is_some() {
                        return Err(JpegError::Unsupported(UnsupportedFeature::Hierarchical));
                    }
                    let frame = parse_sof(&mut self.input, n)?;
                    let component_count = frame.components.len();

                    if frame.is_differential {
                        return Err(JpegError::Unsupported(UnsupportedFeature::Hierarchical));
                    }
                    if frame.coding_process == CodingProcess::Lossless {
                        return Err(JpegError::Unsupported(UnsupportedFeature::Lossless));
                    }
                    if frame.entropy_coding == EntropyCoding::Arithmetic {
                        return Err(JpegError::Unsupported(
                            UnsupportedFeature::ArithmeticEntropyCoding,
                        ));
                    }
                    // TODO: Do we support higher precision?
                    if frame.precision != 8 {
                        return Err(JpegError::Unsupported(UnsupportedFeature::SamplePrecision(
                            frame.precision,
                        )));
                    }
                    if frame.size.height == 0 {
                        return Err(JpegError::Unsupported(UnsupportedFeature::DNL));
                    }
                    if component_count != 1 && component_count != 3 && component_count != 4 {
                        return Err(JpegError::Unsupported(UnsupportedFeature::ComponentCount(
                            component_count as u8,
                        )));
                    }
                    self.frame = Some(frame);
                }
                // Scan header
                Marker::SOS => {
                    if self.frame.is_none() {
                        return Err(JpegError::Malformatted(
                            "scan encountered before frame".to_owned(),
                        ));
                    }
                    let frame = &self.frame.clone().unwrap();
                    let scan = parse_sos(&mut self.input, frame)?;
                    if self.coefficients.is_empty() {
                        self.coefficients = frame
                            .components
                            .iter()
                            .map(|c| {
                                let block_count = c.size_in_block.width as usize
                                    * c.size_in_block.height as usize;
                                vec![0; block_count * 64]
                            })
                            .collect();
                    }
                    if scan.successive_approximation_low == 0 {
                        for &i in scan.component_indices.iter() {
                            for j in scan.spectral_selection.clone() {
                                self.coefficient_finished[i] |= 1 << j;
                            }
                        }
                    }
                    // TODO: Handle EOF
                    pending_marker = self.decode_scan(frame, &scan)?;
                    scans_processed += 1;
                }
                // Table-specification and miscellaneous markers
                // Quantization table-specification
                Marker::DQT => {
                    parse_dqt(&mut self.input, &mut self.quantization_tables)?;
                    // TODO: Do we need to unsigzag?
                    // for (i, &table) in tables.into_iter().enumerate() {
                    //     if let Some(table) = table {
                    //         let mut unzigzagged_table = [0u16; 64];
                    //         for j in 0..64 {
                    //             unzigzagged_table[UNZIGZAG[j] as usize] = table[j];
                    //         }
                    //         self.quantization_tables[i] = Some(Arc::new(unzigzagged_table));
                    //     }
                    // }
                }
                // Huffman table-specification
                Marker::DHT => {
                    let is_baseline = self.frame.as_ref().map(|frame| frame.is_baseline);
                    /* let (dc_tables, ac_tables) = */
                    parse_dht(
                        &mut self.input,
                        is_baseline,
                        &mut self.dc_huffman_tables,
                        &mut self.ac_huffman_tables,
                    )?;
                    // let current_dc_tables = mem::replace(&mut self.dc_huffman_tables, vec![]);
                    // self.dc_huffman_tables = dc_tables
                    //     .into_iter()
                    //     .zip(current_dc_tables.into_iter())
                    //     .map(|(a, b)| a.or(b))
                    //     .collect();
                    // let current_ac_tables = mem::replace(&mut self.ac_huffman_tables, vec![]);
                    // self.ac_huffman_tables = ac_tables
                    //     .into_iter()
                    //     .zip(current_ac_tables.into_iter())
                    //     .map(|(a, b)| a.or(b))
                    //     .collect();
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
                Marker::APP(..) => {
                    if let Some(data) = parse_app(&mut self.input, marker)? {
                        match data {
                            AppData::Adobe(color_transform) => {
                                self.color_transform = Some(color_transform)
                            }
                            AppData::Jfif => {
                                // From the JFIF spec:
                                // "The APP0 marker is used to identify a JPEG FIF file.
                                //     The JPEG FIF APP0 marker is mandatory right after the SOI marker."
                                // Some JPEGs in the wild does not follow this though, so we allow
                                // JFIF headers anywhere APP0 markers are allowed.
                                /*
                                if previous_marker != Marker::SOI {
                                    return Err(JpegError::Format("the JFIF APP0 marker must come right after the SOI marker"));
                                }
                                */
                                self.is_jfif = true;
                            }
                            AppData::Avi1 => self.is_mjpeg = true,
                        }
                    }
                }
                // Restart
                Marker::RST(..) => {
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
                    if previous_marker != Marker::SOS || scans_processed != 1 {
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

    fn decode_scan(&mut self, frame: &FrameInfo, scan: &ScanInfo) -> JpegResult<Option<Marker>> {
        assert!(scan.component_indices.len() <= MAX_COMPONENTS);
        let components: Vec<Component> = scan.component_indices
            .iter()
            .map(|&i| frame.components[i].clone())
            .collect();
        // Verify that all required quantization tables has been set.
        if components
            .iter()
            .any(|component| self.quantization_tables[component.quantization_table_index].is_none())
        {
            return Err(JpegError::Malformatted(
                "use of unset quantization table".to_owned(),
            ));
        }
        if self.is_mjpeg {
            fill_default_mjpeg_tables(
                scan,
                &mut self.dc_huffman_tables,
                &mut self.ac_huffman_tables,
            );
        }
        // Verify that all required huffman tables has been set.
        if scan.spectral_selection.start == 0
            && scan.dc_table_indices
                .iter()
                .any(|&i| self.dc_huffman_tables[i].is_none())
        {
            return Err(JpegError::Malformatted(
                "scan makes use of unset dc huffman table".to_owned(),
            ));
        }
        if scan.spectral_selection.end > 1
            && scan.ac_table_indices
                .iter()
                .any(|&i| self.ac_huffman_tables[i].is_none())
        {
            return Err(JpegError::Malformatted(
                "scan makes use of unset ac huffman table".to_owned(),
            ));
        }
        let is_interleaved = components.len() > 1;
        let mut huffman = HuffmanDecoder::new();
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
            // TODO: huffman.overhang_byte(self.input) and handle EOF
            // If EOF when mcu_y == 0 discard scan
            for mcu_x in 0..size_in_mcu.width as usize {
                if is_interleaved {
                    for (i, component) in components.iter().enumerate() {
                        for block_y_offset in 0..component.vertical_sampling_factor as usize {
                            for block_x_offset in 0..component.horizontal_sampling_factor as usize {
                                let block_y = mcu_y * component.vertical_sampling_factor as usize
                                    + block_y_offset;
                                let block_x = mcu_x * component.horizontal_sampling_factor as usize
                                    + block_x_offset;
                                // TODO: Deal with EOF
                                self.decode_block(
                                    block_y,
                                    block_x,
                                    i,
                                    component,
                                    scan,
                                    &mut huffman,
                                    &mut eob_run,
                                    &mut dc_predictors[i],
                                )?;
                            }
                        }
                    }
                } else {
                    // TODO: Deal with EOF
                    self.decode_block(
                        mcu_y,
                        mcu_x,
                        0,
                        &components[0],
                        scan,
                        &mut huffman,
                        &mut eob_run,
                        &mut dc_predictors[0],
                    )?;
                }
                if self.restart_interval > 0 {
                    n_mcu_left_until_restart -= 1;
                    let is_last_mcu = mcu_x as u16 == frame.size_in_mcu.width - 1
                        && mcu_y as u16 == frame.size_in_mcu.height - 1;
                    if n_mcu_left_until_restart == 0 && !is_last_mcu {
                        // TODO: Deal with EOF
                        match huffman.take_marker(&mut self.input)? {
                            Some(Marker::RST(n)) => {
                                if n != expected_rst_num {
                                    return Err(JpegError::Malformatted(format!(
                                        "found RST{} where RST{} was expected",
                                        n, expected_rst_num
                                    )));
                                }
                                huffman.reset();
                                // Section F.2.1.3.1
                                dc_predictors = [0i16; MAX_COMPONENTS];
                                // Section G.1.2.2
                                eob_run = 0;
                                expected_rst_num = (expected_rst_num + 1) % 8;
                                n_mcu_left_until_restart = self.restart_interval;
                            }
                            Some(marker) => {
                                return Err(JpegError::Malformatted(format!(
                                    "found marker {:?} inside scan where RST{} was expected",
                                    marker, expected_rst_num
                                )))
                            }
                            None => {
                                return Err(JpegError::Malformatted(format!(
                                    "no marker found where RST{} was expected",
                                    expected_rst_num
                                )))
                            }
                        }
                    }
                }
            }
        }
        // TODO: Deal with EOF
        huffman.take_marker(&mut self.input)
    }

    fn decode_block(
        &mut self,
        y: usize,
        x: usize,
        component_index: usize,
        component: &Component,
        scan: &ScanInfo,
        huffman: &mut HuffmanDecoder,
        eob_run: &mut u16,
        dc_predictor: &mut i16,
    ) -> JpegResult<()> {
        let block_offset = (y * component.size_in_block.width as usize + x) * 64;
        let coefficients = &mut self.coefficients[scan.component_indices[component_index]]
            [block_offset..block_offset + 64];
        // TODO: huffman.clear_buffer();
        if scan.successive_approximation_high == 0 {
            decode_block(
                &mut self.input,
                coefficients,
                huffman,
                self.dc_huffman_tables[scan.dc_table_indices[component_index]].as_ref().unwrap(),
                self.ac_huffman_tables[scan.ac_table_indices[component_index]].as_ref().unwrap(),
                scan.spectral_selection.clone(),
                scan.successive_approximation_low,
                eob_run,
                dc_predictor,
            )
        } else {
            decode_block_successive_approximation(
                &mut self.input,
                coefficients,
                huffman,
                self.ac_huffman_tables[scan.ac_table_indices[component_index]].as_ref().unwrap(),
                scan.spectral_selection.clone(),
                scan.successive_approximation_low,
                eob_run,
            )
        }
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
    let mut index = cmp::max(spectral_selection.start, 1);
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
                _ => return Err(JpegError::Malformatted("unexpected huffman code".to_owned())),
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
