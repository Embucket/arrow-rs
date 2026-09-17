// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use arrow_schema::ArrowError;
use csv_core::{ReadRecordResult, Reader};
use std::sync::Arc;

use super::{CsvRecord, CsvRecordError, CsvRecordErrorHandler};

/// The estimated length of a field in bytes
const AVERAGE_FIELD_SIZE: usize = 8;

/// The minimum amount of data in a single read
const MIN_CAPACITY: usize = 1024;

/// Prevent malformed records from growing the field-offset buffer without bound.
///
/// This is larger than typical schema limits and only applies after a record has already
/// exceeded the configured schema width.
const MAX_RECOVERABLE_EXCESS_FIELDS: usize = 16_384;

/// [`RecordDecoder`] provides a push-based interface to decoder [`StringRecords`]
#[derive(Debug)]
pub struct RecordDecoder {
    delimiter: Reader,

    /// The expected number of fields per row
    num_columns: usize,

    /// The current line number
    line_number: usize,

    /// Offsets delimiting field start positions
    offsets: Vec<usize>,

    /// The current offset into `self.offsets`
    ///
    /// We track this independently of Vec to avoid re-zeroing memory
    offsets_len: usize,

    /// The number of fields read for the current record
    current_field: usize,

    /// The number of rows buffered
    num_rows: usize,

    /// Decoded field data
    data: Vec<u8>,

    /// Offsets into data
    ///
    /// We track this independently of Vec to avoid re-zeroing memory
    data_len: usize,

    /// Whether rows with less than expected columns are considered valid
    ///
    /// Default value is false
    /// When enabled fills in missing columns with null
    truncated_rows: bool,

    /// Optional recovery hook. Keeping this `None` selects the allocation-free strict path.
    record_error_handler: Option<Arc<dyn CsvRecordErrorHandler>>,

    /// Raw bytes for the current record, retained only by the recovery path.
    record_bytes: Vec<u8>,

    /// Total number of input bytes consumed by the recovery path.
    stream_offset: usize,

    /// Start offsets for rolling back a malformed record from decoded output.
    record_byte_offset: usize,
    record_data_start: usize,
    record_offsets_start: usize,
}

impl RecordDecoder {
    pub fn new(delimiter: Reader, num_columns: usize, truncated_rows: bool) -> Self {
        Self {
            delimiter,
            num_columns,
            line_number: 1,
            offsets: vec![],
            offsets_len: 1, // The first offset is always 0
            current_field: 0,
            data_len: 0,
            data: vec![],
            num_rows: 0,
            truncated_rows,
            record_error_handler: None,
            record_bytes: vec![],
            stream_offset: 0,
            record_byte_offset: 0,
            record_data_start: 0,
            record_offsets_start: 1,
        }
    }

    pub fn with_record_error_handler(
        mut self,
        handler: Option<Arc<dyn CsvRecordErrorHandler>>,
    ) -> Self {
        self.record_error_handler = handler;
        self
    }

    /// Decodes records from `input` returning the number of records and bytes read
    ///
    /// Note: this expects to be called with an empty `input` to signal EOF
    pub fn decode(&mut self, input: &[u8], to_read: usize) -> Result<(usize, usize), ArrowError> {
        match self.record_error_handler.clone() {
            Some(handler) => {
                self.decode_with_record_error_handler(input, to_read, handler.as_ref())
            }
            None => self.decode_strict(input, to_read),
        }
    }

    fn decode_strict(
        &mut self,
        input: &[u8],
        to_read: usize,
    ) -> Result<(usize, usize), ArrowError> {
        if to_read == 0 {
            return Ok((0, 0));
        }

        // Reserve sufficient capacity in offsets
        self.offsets
            .resize(self.offsets_len + to_read * self.num_columns, 0);

        // The current offset into `input`
        let mut input_offset = 0;

        // The number of rows decoded in this pass
        let mut read = 0;

        loop {
            // Reserve necessary space in output data based on best estimate
            let remaining_rows = to_read - read;
            let capacity = remaining_rows * self.num_columns * AVERAGE_FIELD_SIZE;
            let estimated_data = capacity.max(MIN_CAPACITY);
            self.data.resize(self.data_len + estimated_data, 0);

            // Try to read a record
            loop {
                let (result, bytes_read, bytes_written, end_positions) =
                    self.delimiter.read_record(
                        &input[input_offset..],
                        &mut self.data[self.data_len..],
                        &mut self.offsets[self.offsets_len..],
                    );

                self.current_field += end_positions;
                self.offsets_len += end_positions;
                input_offset += bytes_read;
                self.data_len += bytes_written;

                match result {
                    ReadRecordResult::End | ReadRecordResult::InputEmpty => {
                        // Reached end of input
                        return Ok((read, input_offset));
                    }
                    // Need to allocate more capacity
                    ReadRecordResult::OutputFull => break,
                    ReadRecordResult::OutputEndsFull => {
                        return Err(ArrowError::CsvError(format!(
                            "incorrect number of fields for line {}, expected {} got more than {}",
                            self.line_number, self.num_columns, self.current_field
                        )));
                    }
                    ReadRecordResult::Record => {
                        if self.current_field != self.num_columns {
                            if self.truncated_rows && self.current_field < self.num_columns {
                                // If the number of fields is less than expected, pad with nulls
                                let fill_count = self.num_columns - self.current_field;
                                let fill_value = self.offsets[self.offsets_len - 1];
                                self.offsets[self.offsets_len..self.offsets_len + fill_count]
                                    .fill(fill_value);
                                self.offsets_len += fill_count;
                            } else {
                                return Err(ArrowError::CsvError(format!(
                                    "incorrect number of fields for line {}, expected {} got {}",
                                    self.line_number, self.num_columns, self.current_field
                                )));
                            }
                        }
                        read += 1;
                        self.current_field = 0;
                        self.line_number += 1;
                        self.num_rows += 1;

                        if read == to_read {
                            // Read sufficient rows
                            return Ok((read, input_offset));
                        }

                        if input.len() == input_offset {
                            // Input exhausted, need to read more
                            // Without this read_record will interpret the empty input
                            // byte array as indicating the end of the file
                            return Ok((read, input_offset));
                        }
                    }
                }
            }
        }
    }

    fn decode_with_record_error_handler(
        &mut self,
        input: &[u8],
        to_read: usize,
        handler: &dyn CsvRecordErrorHandler,
    ) -> Result<(usize, usize), ArrowError> {
        if to_read == 0 {
            return Ok((0, 0));
        }

        let required_offsets = to_read
            .checked_mul(self.num_columns)
            .and_then(|additional| self.offsets_len.checked_add(additional))
            .ok_or_else(|| ArrowError::CsvError("CSV field offset capacity overflowed".into()))?;
        self.offsets.resize(required_offsets, 0);
        let mut input_offset = 0;
        let mut read = 0;

        loop {
            let remaining_rows = to_read - read;
            let capacity = remaining_rows * self.num_columns * AVERAGE_FIELD_SIZE;
            let estimated_data = capacity.max(MIN_CAPACITY);
            self.data.resize(self.data_len + estimated_data, 0);

            loop {
                let max_offsets_len = self
                    .record_offsets_start
                    .checked_add(self.num_columns)
                    .and_then(|expected| expected.checked_add(MAX_RECOVERABLE_EXCESS_FIELDS))
                    .ok_or_else(|| {
                        ArrowError::CsvError("CSV field offset capacity overflowed".into())
                    })?;
                let offsets_end = self.offsets.len().min(max_offsets_len);
                let record_input_start = input_offset;
                let (result, bytes_read, bytes_written, end_positions) =
                    self.delimiter.read_record(
                        &input[input_offset..],
                        &mut self.data[self.data_len..],
                        &mut self.offsets[self.offsets_len..offsets_end],
                    );

                self.current_field += end_positions;
                self.offsets_len += end_positions;
                input_offset += bytes_read;
                self.data_len += bytes_written;
                self.stream_offset =
                    self.stream_offset.checked_add(bytes_read).ok_or_else(|| {
                        ArrowError::CsvError("CSV input byte offset overflowed".into())
                    })?;
                self.record_bytes
                    .extend_from_slice(&input[record_input_start..input_offset]);

                match result {
                    ReadRecordResult::End | ReadRecordResult::InputEmpty => {
                        return Ok((read, input_offset));
                    }
                    ReadRecordResult::OutputFull => break,
                    ReadRecordResult::OutputEndsFull => {
                        if self.offsets_len >= max_offsets_len {
                            return Err(recovery_limit_error(self.line_number));
                        }
                        let new_len = self
                            .offsets_len
                            .checked_add(self.num_columns.max(1))
                            .ok_or_else(|| {
                                ArrowError::CsvError("CSV field offset capacity overflowed".into())
                            })?
                            .min(max_offsets_len);
                        self.offsets.resize(new_len, 0);
                    }
                    ReadRecordResult::Record => {
                        if self.current_field != self.num_columns
                            && !(self.truncated_rows && self.current_field < self.num_columns)
                        {
                            handler.handle(&CsvRecordError {
                                line_number: self.line_number,
                                byte_offset: self.record_byte_offset,
                                expected_fields: self.num_columns,
                                actual_fields: self.current_field,
                                record: &self.record_bytes,
                            })?;
                            self.data_len = self.record_data_start;
                            self.offsets_len = self.record_offsets_start;
                            self.current_field = 0;
                            self.line_number += 1;
                            self.record_bytes.clear();
                            self.record_byte_offset = self.stream_offset;
                            continue;
                        }

                        handler.handle_record(&CsvRecord {
                            line_number: self.line_number,
                            byte_offset: self.record_byte_offset,
                            record: &self.record_bytes,
                        })?;

                        if self.current_field < self.num_columns {
                            let fill_count = self.num_columns - self.current_field;
                            let fill_value = self.offsets[self.offsets_len - 1];
                            self.offsets[self.offsets_len..self.offsets_len + fill_count]
                                .fill(fill_value);
                            self.offsets_len += fill_count;
                        }
                        read += 1;
                        self.current_field = 0;
                        self.line_number += 1;
                        self.num_rows += 1;
                        self.record_bytes.clear();
                        self.record_byte_offset = self.stream_offset;
                        self.record_data_start = self.data_len;
                        self.record_offsets_start = self.offsets_len;

                        if read == to_read {
                            return Ok((read, input_offset));
                        }
                        if input.len() == input_offset {
                            return Ok((read, input_offset));
                        }
                    }
                }
            }
        }
    }

    /// Returns the current number of buffered records
    pub fn len(&self) -> usize {
        self.num_rows
    }

    /// Returns true if the decoder is empty
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Clears the current contents of the decoder
    pub fn clear(&mut self) {
        // This does not reset current_field to allow clearing part way through a record
        self.offsets_len = 1;
        self.data_len = 0;
        self.num_rows = 0;
        self.record_data_start = 0;
        self.record_offsets_start = 1;
    }

    /// Flushes the current contents of the reader
    pub fn flush(&mut self) -> Result<StringRecords<'_>, ArrowError> {
        if self.current_field != 0 {
            return Err(ArrowError::CsvError(
                "Cannot flush part way through record".to_string(),
            ));
        }

        // csv_core::Reader writes end offsets relative to the start of the row
        // Therefore scan through and offset these based on the cumulative row offsets
        let mut row_offset: usize = 0;
        self.offsets[1..self.offsets_len]
            .chunks_exact_mut(self.num_columns)
            .try_for_each(|row| -> Result<(), ArrowError> {
                let offset = row_offset;
                row.iter_mut().try_for_each(|x| -> Result<(), ArrowError> {
                    *x = x.checked_add(offset).ok_or_else(|| {
                        ArrowError::CsvError(
                            "CSV record offsets overflowed usize while flushing".to_string(),
                        )
                    })?;
                    row_offset = *x;
                    Ok(())
                })
            })?;

        // Need to truncate data t1o the actual amount of data read
        let data = std::str::from_utf8(&self.data[..self.data_len]).map_err(|e| {
            let valid_up_to = e.valid_up_to();

            // We can't use binary search because of empty fields
            let idx = self.offsets[..self.offsets_len]
                .iter()
                .rposition(|x| *x <= valid_up_to)
                .unwrap();

            let field = idx % self.num_columns + 1;
            let line_offset = self.line_number - self.num_rows;
            let line = line_offset + idx / self.num_columns;

            ArrowError::CsvError(format!(
                "Encountered invalid UTF-8 data for line {line} and field {field}"
            ))
        })?;

        let offsets = &self.offsets[..self.offsets_len];
        let num_rows = self.num_rows;

        // Reset state
        self.offsets_len = 1;
        self.data_len = 0;
        self.num_rows = 0;
        self.record_data_start = 0;
        self.record_offsets_start = 1;

        Ok(StringRecords {
            num_rows,
            num_columns: self.num_columns,
            offsets,
            data,
        })
    }
}

fn recovery_limit_error(line_number: usize) -> ArrowError {
    ArrowError::CsvError(format!(
        "malformed CSV record on line {line_number} exceeds the recovery limit of {MAX_RECOVERABLE_EXCESS_FIELDS} excess fields"
    ))
}

/// A collection of parsed, UTF-8 CSV records
#[derive(Debug)]
pub struct StringRecords<'a> {
    num_columns: usize,
    num_rows: usize,
    offsets: &'a [usize],
    data: &'a str,
}

impl<'a> StringRecords<'a> {
    fn get(&self, index: usize) -> StringRecord<'a> {
        let field_idx = index * self.num_columns;
        StringRecord {
            data: self.data,
            offsets: &self.offsets[field_idx..field_idx + self.num_columns + 1],
        }
    }

    pub fn len(&self) -> usize {
        self.num_rows
    }

    pub fn iter(&self) -> impl Iterator<Item = StringRecord<'a>> + '_ {
        (0..self.num_rows).map(|x| self.get(x))
    }
}

/// A single parsed, UTF-8 CSV record
#[derive(Debug, Clone, Copy)]
pub struct StringRecord<'a> {
    data: &'a str,
    offsets: &'a [usize],
}

impl<'a> StringRecord<'a> {
    pub fn get(&self, index: usize) -> &'a str {
        let end = self.offsets[index + 1];
        let start = self.offsets[index];

        // SAFETY:
        // Parsing produces offsets at valid byte boundaries
        unsafe { self.data.get_unchecked(start..end) }
    }
}

impl std::fmt::Display for StringRecord<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_fields = self.offsets.len() - 1;
        write!(f, "[")?;
        for i in 0..num_fields {
            if i > 0 {
                write!(f, ",")?;
            }
            write!(f, "{}", self.get(i))?;
        }
        write!(f, "]")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::reader::{CsvRecord, CsvRecordError, CsvRecordErrorHandler};
    use arrow_schema::ArrowError;
    use csv_core::Reader;
    use std::io::{BufRead, BufReader, Cursor};
    use std::sync::{Arc, Mutex};

    use super::{MAX_RECOVERABLE_EXCESS_FIELDS, RecordDecoder, recovery_limit_error};

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct OwnedRecordError {
        line_number: usize,
        byte_offset: usize,
        expected_fields: usize,
        actual_fields: usize,
        record: Vec<u8>,
    }

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct OwnedRecord {
        line_number: usize,
        byte_offset: usize,
        record: Vec<u8>,
    }

    #[derive(Debug, Default)]
    struct CollectRecords {
        errors: Mutex<Vec<OwnedRecordError>>,
        records: Mutex<Vec<OwnedRecord>>,
    }

    impl CsvRecordErrorHandler for CollectRecords {
        fn handle(&self, error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
            self.errors.lock().unwrap().push(OwnedRecordError {
                line_number: error.line_number,
                byte_offset: error.byte_offset,
                expected_fields: error.expected_fields,
                actual_fields: error.actual_fields,
                record: error.record.to_vec(),
            });
            Ok(())
        }

        fn handle_record(&self, record: &CsvRecord<'_>) -> Result<(), ArrowError> {
            self.records.lock().unwrap().push(OwnedRecord {
                line_number: record.line_number,
                byte_offset: record.byte_offset,
                record: record.record.to_vec(),
            });
            Ok(())
        }
    }

    #[test]
    fn test_basic() {
        let csv = [
            "foo,bar,baz",
            "a,b,c",
            "12,3,5",
            "\"asda\"\"asas\",\"sdffsnsd\", as",
        ]
        .join("\n");

        let mut expected = vec![
            vec!["foo", "bar", "baz"],
            vec!["a", "b", "c"],
            vec!["12", "3", "5"],
            vec!["asda\"asas", "sdffsnsd", " as"],
        ]
        .into_iter();

        let mut reader = BufReader::with_capacity(3, Cursor::new(csv.as_bytes()));
        let mut decoder = RecordDecoder::new(Reader::new(), 3, false);

        loop {
            let to_read = 3;
            let mut read = 0;
            loop {
                let buf = reader.fill_buf().unwrap();
                let (records, bytes) = decoder.decode(buf, to_read - read).unwrap();

                reader.consume(bytes);
                read += records;

                if read == to_read || bytes == 0 {
                    break;
                }
            }
            if read == 0 {
                break;
            }

            let b = decoder.flush().unwrap();
            b.iter().zip(&mut expected).for_each(|(record, expected)| {
                let actual = (0..3)
                    .map(|field_idx| record.get(field_idx))
                    .collect::<Vec<_>>();
                assert_eq!(actual, expected)
            });
        }
        assert!(expected.next().is_none());
    }

    #[test]
    fn test_invalid_fields() {
        let csv = "a,b\nb,c\na\n";
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false);
        let err = decoder.decode(csv.as_bytes(), 4).unwrap_err().to_string();

        let expected = "Csv error: incorrect number of fields for line 3, expected 2 got 1";

        assert_eq!(err, expected);

        // Test with initial skip
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false);
        let (skipped, bytes) = decoder.decode(csv.as_bytes(), 1).unwrap();
        assert_eq!(skipped, 1);
        decoder.clear();

        let remaining = &csv.as_bytes()[bytes..];
        let err = decoder.decode(remaining, 3).unwrap_err().to_string();
        assert_eq!(err, expected);
    }

    #[test]
    fn test_invalid_fields_handler_skips_records_across_input_chunks() {
        let csv = b"1,ok\n2,extra,value\n3\n4,after\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let mut reader = BufReader::with_capacity(3, Cursor::new(csv));

        loop {
            let buf = reader.fill_buf().unwrap();
            let (_, bytes) = decoder.decode(buf, 4 - decoder.len()).unwrap();
            reader.consume(bytes);
            if bytes == 0 || decoder.len() == 4 {
                break;
            }
        }

        let records = decoder.flush().unwrap();
        let actual = records
            .iter()
            .map(|record| [record.get(0).to_owned(), record.get(1).to_owned()])
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            [
                [String::from("1"), String::from("ok")],
                [String::from("4"), String::from("after")],
            ]
        );

        let errors = handler.errors.lock().unwrap();
        assert_eq!(
            *errors,
            [
                OwnedRecordError {
                    line_number: 2,
                    byte_offset: 5,
                    expected_fields: 2,
                    actual_fields: 3,
                    record: b"2,extra,value\n".to_vec(),
                },
                OwnedRecordError {
                    line_number: 3,
                    byte_offset: 19,
                    expected_fields: 2,
                    actual_fields: 1,
                    record: b"3\n".to_vec(),
                },
            ]
        );
        drop(errors);

        let records = handler.records.lock().unwrap();
        assert_eq!(
            *records,
            [
                OwnedRecord {
                    line_number: 1,
                    byte_offset: 0,
                    record: b"1,ok\n".to_vec(),
                },
                OwnedRecord {
                    line_number: 4,
                    byte_offset: 21,
                    record: b"4,after\n".to_vec(),
                },
            ]
        );
    }

    #[test]
    fn test_invalid_fields_handler_bounds_offsets_across_input_chunks() {
        let mut csv = Vec::with_capacity(2 * (MAX_RECOVERABLE_EXCESS_FIELDS + 3));
        for _ in 0..MAX_RECOVERABLE_EXCESS_FIELDS + 3 {
            csv.extend_from_slice(b"x,");
        }
        csv.push(b'\n');

        let handler = Arc::new(CollectRecords::default());
        let mut decoder =
            RecordDecoder::new(Reader::new(), 2, false).with_record_error_handler(Some(handler));
        let mut input_offset = 0_usize;
        let error = loop {
            let input_end = input_offset.saturating_add(17).min(csv.len());
            match decoder.decode(&csv[input_offset..input_end], 1) {
                Ok((_, bytes_read)) => {
                    assert!(bytes_read > 0);
                    input_offset += bytes_read;
                }
                Err(error) => break error,
            }
        };

        assert_eq!(error.to_string(), recovery_limit_error(1).to_string());
        assert_eq!(decoder.offsets.len(), 1 + 2 + MAX_RECOVERABLE_EXCESS_FIELDS);
    }

    #[test]
    fn test_skip_insufficient_rows() {
        let csv = "a\nv\n";
        let mut decoder = RecordDecoder::new(Reader::new(), 1, false);
        let (read, bytes) = decoder.decode(csv.as_bytes(), 3).unwrap();
        assert_eq!(read, 2);
        assert_eq!(bytes, csv.len());
    }

    #[test]
    fn test_truncated_rows() {
        let csv = "a,b\nv\n,1\n,2\n,3\n";
        let mut decoder = RecordDecoder::new(Reader::new(), 2, true);
        let (read, bytes) = decoder.decode(csv.as_bytes(), 5).unwrap();
        assert_eq!(read, 5);
        assert_eq!(bytes, csv.len());
    }

    /// Regression test for an overflow path found by the `arrow-csv`
    /// cargo-fuzz harness being prototyped for #5332. Stages the
    /// `RecordDecoder` state directly so that rebasing the second row's
    /// end offset overflows `usize`. With the previous `*x += offset` this
    /// panicked with `attempt to add with overflow`; the patched code
    /// surfaces the condition as `ArrowError::CsvError`.
    #[test]
    fn test_flush_offset_overflow_returns_csv_error() {
        let mut decoder = RecordDecoder::new(Reader::new(), 1, false);
        decoder.offsets = vec![0, usize::MAX, 1];
        decoder.offsets_len = 3;
        decoder.num_rows = 2;
        let err = decoder.flush().unwrap_err();
        assert_eq!(
            err.to_string(),
            "Csv error: CSV record offsets overflowed usize while flushing"
        );
    }
}
