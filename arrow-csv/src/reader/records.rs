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

use super::{
    CsvFilterRecord, CsvInvalidUtf8Error, CsvRecord, CsvRecordError, CsvRecordErrorHandler,
    NullRegex,
};

#[derive(Debug)]
struct QuotedEmptyFields {
    delimiter: u8,
    quote: u8,
    escape: Option<u8>,
    comment: Option<u8>,
    terminator: Option<u8>,

    fields: Vec<u8>,
}

fn ignored_record_prefix(raw: &[u8], comment: Option<u8>, terminator: Option<u8>) -> usize {
    let mut offset = 0;
    while let Some(&byte) = raw.get(offset) {
        if Some(byte) == comment {
            let Some(end) = raw[offset..].iter().position(|byte| *byte == b'\n') else {
                break;
            };
            offset += end + 1;
        } else if terminator.is_some_and(|term| byte == term) {
            offset += 1;
        } else if terminator.is_none() && byte == b'\r' {
            offset += 1;
            offset += usize::from(raw.get(offset) == Some(&b'\n'));
        } else if terminator.is_none() && byte == b'\n' {
            offset += 1;
        } else {
            break;
        }
    }
    offset
}

fn count_source_line_breaks(raw: &[u8]) -> usize {
    memchr::memchr2_iter(b'\r', b'\n', raw)
        .filter(|&index| raw[index] != b'\r' || raw.get(index + 1) != Some(&b'\n'))
        .count()
}

#[derive(Clone, Copy, Debug)]
struct SourceLineCounter {
    initial_line_offset: usize,
    lf_count: usize,
    count_lf_independently: bool,
    bare_cr_count: usize,
    pending_cr: bool,
}

impl SourceLineCounter {
    fn advance(&mut self, bytes: &[u8]) {
        if self.count_lf_independently {
            self.lf_count = self
                .lf_count
                .saturating_add(memchr::memchr_iter(b'\n', bytes).count());
        }
        if let Some(&first) = bytes.first() {
            if self.pending_cr && first != b'\n' {
                self.bare_cr_count = self.bare_cr_count.saturating_add(1);
            }
            self.pending_cr = false;
        }
        for index in memchr::memchr_iter(b'\r', bytes) {
            match bytes.get(index + 1) {
                Some(b'\n') => {}
                None => self.pending_cr = true,
                _ => self.bare_cr_count = self.bare_cr_count.saturating_add(1),
            }
        }
    }

    fn record_start_line(self, parser_line: usize, record: &[u8]) -> usize {
        let lf_line = if self.count_lf_independently {
            self.lf_count.saturating_add(1)
        } else {
            parser_line
        };
        let end_line = lf_line
            .saturating_add(self.bare_cr_count)
            .saturating_add(usize::from(
                self.pending_cr && record.last() == Some(&b'\r'),
            ));
        end_line
            .saturating_sub(count_source_line_breaks(record))
            .saturating_add(self.initial_line_offset)
    }
}

impl QuotedEmptyFields {
    fn record(
        &mut self,
        row: usize,
        columns: usize,
        present_fields: usize,
        ignore_extra_fields: bool,
        raw: &[u8],
    ) -> Result<(), ArrowError> {
        let start = row
            .checked_mul(columns)
            .ok_or_else(|| ArrowError::CsvError("CSV quoted-field offset overflowed".to_owned()))?;
        let end = start
            .checked_add(columns)
            .ok_or_else(|| ArrowError::CsvError("CSV quoted-field offset overflowed".to_owned()))?;
        if self.fields.len() < end {
            self.fields.resize(end, 0);
        }
        let flags = &mut self.fields[start..end];
        flags.fill(0);

        let mut field = 0;
        let mut start = 0;
        let mut in_quotes = false;
        let mut terminated = false;
        let mut i = 0;
        while i < raw.len() {
            let byte = raw[i];
            if field == 0
                && i == start
                && (self.terminator.is_some_and(|term| byte == term)
                    || (self.terminator.is_none() && (byte == b'\n' || byte == b'\r')))
            {
                i += 1;
                start = i;
                continue;
            }
            if field == 0 && i == start && Some(byte) == self.comment {
                while i < raw.len() && raw[i] != b'\n' {
                    i += 1;
                }
                i += usize::from(i < raw.len());
                start = i;
                continue;
            }
            if in_quotes {
                if byte == self.quote {
                    if raw.get(i + 1) == Some(&self.quote) {
                        i += 2;
                        continue;
                    }
                    in_quotes = false;
                } else if Some(byte) == self.escape && i + 1 < raw.len() {
                    i += 2;
                    continue;
                }
            } else if i == start && byte == self.quote {
                in_quotes = true;
            } else if byte == self.delimiter
                || self.terminator.is_some_and(|term| byte == term)
                || (self.terminator.is_none() && (byte == b'\n' || byte == b'\r'))
            {
                if let Some(flag) = flags.get_mut(field) {
                    *flag = u8::from(raw[start..i] == [self.quote, self.quote]);
                } else if !ignore_extra_fields {
                    return Err(ArrowError::CsvError(
                        "CSV quoted-field count exceeds schema".to_owned(),
                    ));
                }
                field += 1;
                start = i + 1;
                if self.terminator.is_some_and(|term| byte == term)
                    || (self.terminator.is_none() && (byte == b'\n' || byte == b'\r'))
                {
                    terminated = true;
                    break;
                }
            }
            i += 1;
        }
        if !terminated {
            if let Some(flag) = flags.get_mut(field) {
                *flag = u8::from(raw[start..] == [self.quote, self.quote]);
            } else if !ignore_extra_fields {
                return Err(ArrowError::CsvError(
                    "CSV quoted-field count exceeds schema".to_owned(),
                ));
            }
            field += 1;
        }
        if field != present_fields {
            return Err(ArrowError::CsvError(format!(
                "CSV quoted-field count mismatch, expected {present_fields} got {field}"
            )));
        }
        Ok(())
    }
}

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

    /// Actual field counts, retained only when short rows are allowed
    field_counts: Option<Vec<usize>>,

    /// Quoted-empty flags, retained only when the caller needs that distinction
    quoted_empty: Option<QuotedEmptyFields>,

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

    /// Discard fields beyond the schema width on the opt-in path.
    ignore_extra_fields: bool,
    skipping_extra_fields: bool,

    /// Optional recovery hook. Keeping this `None` selects the allocation-free strict path.
    record_error_handler: Option<Arc<dyn CsvRecordErrorHandler>>,

    /// Cache this capability once so the common recovery path avoids another virtual call.
    filter_records: bool,
    record_source_lines: bool,

    /// Opt-in bound for a malformed record that never reaches a terminator.
    max_record_bytes: Option<usize>,

    /// Opt-in replacement of malformed decoded UTF-8 fields.
    replace_invalid_utf8: bool,

    /// One-based physical source line, kept separate from the record ordinal.
    source_lines: SourceLineCounter,

    /// Ignore record callbacks while skipping unvalidated header rows.
    skipping_records: bool,

    /// CSV syntax used to trim comments and blank lines from recovery callbacks.
    comment: Option<u8>,
    terminator: Option<u8>,

    /// A valid UTF-8 input buffer remains valid after removing ASCII CSV syntax.
    utf8_preserving_syntax: bool,
    record_needs_utf8_validation: bool,

    /// Raw bytes for the current record, retained only by the recovery path.
    record_bytes: Vec<u8>,

    /// Total number of input bytes consumed by the recovery path.
    stream_offset: usize,
    initial_stream_offset: usize,

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
            field_counts: truncated_rows.then(Vec::new),
            quoted_empty: None,
            truncated_rows,
            ignore_extra_fields: false,
            skipping_extra_fields: false,
            record_error_handler: None,
            filter_records: false,
            record_source_lines: false,
            max_record_bytes: None,
            replace_invalid_utf8: false,
            source_lines: SourceLineCounter {
                initial_line_offset: 0,
                lf_count: 0,
                count_lf_independently: true,
                bare_cr_count: 0,
                pending_cr: false,
            },
            skipping_records: false,
            comment: None,
            terminator: None,
            utf8_preserving_syntax: false,
            record_needs_utf8_validation: false,
            record_bytes: vec![],
            stream_offset: 0,
            initial_stream_offset: 0,
            record_byte_offset: 0,
            record_data_start: 0,
            record_offsets_start: 1,
        }
    }

    pub(crate) fn with_initial_source_position(
        mut self,
        skipped_lines: usize,
        skipped_bytes: usize,
    ) -> Self {
        self.source_lines.initial_line_offset = skipped_lines;
        self.stream_offset = skipped_bytes;
        self.initial_stream_offset = skipped_bytes;
        self.record_byte_offset = skipped_bytes;
        self
    }

    pub fn with_ignore_extra_fields(mut self, ignore: bool) -> Self {
        self.ignore_extra_fields = ignore;
        self
    }

    pub fn with_record_error_handler(
        mut self,
        handler: Option<Arc<dyn CsvRecordErrorHandler>>,
    ) -> Self {
        self.filter_records = handler
            .as_ref()
            .is_some_and(|handler| handler.filters_records());
        self.record_source_lines = handler
            .as_ref()
            .is_some_and(|handler| handler.needs_source_line_number());
        self.max_record_bytes = handler
            .as_ref()
            .and_then(|handler| handler.max_record_bytes());
        self.record_error_handler = handler;
        self
    }

    pub fn with_replace_invalid_utf8(mut self, replace: bool) -> Self {
        self.replace_invalid_utf8 = replace;
        self
    }

    pub(super) fn with_ignored_prefix_options(
        mut self,
        comment: Option<u8>,
        terminator: Option<u8>,
    ) -> Self {
        self.comment = comment;
        self.terminator = terminator;
        self.source_lines.count_lf_independently = terminator.is_some();
        self
    }

    pub(super) fn with_utf8_preserving_syntax(mut self, enabled: bool) -> Self {
        self.utf8_preserving_syntax = enabled;
        self
    }

    pub fn with_quoted_empty_tracking(
        mut self,
        delimiter: u8,
        quote: u8,
        escape: Option<u8>,
        comment: Option<u8>,
        terminator: Option<u8>,
    ) -> Self {
        self.quoted_empty = Some(QuotedEmptyFields {
            delimiter,
            quote,
            escape,
            comment,
            terminator,
            fields: Vec::new(),
        });
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
            None if self.ignore_extra_fields => self.decode_strict::<true>(input, to_read),
            None => self.decode_strict::<false>(input, to_read),
        }
    }

    pub(super) fn decode_skipping(
        &mut self,
        input: &[u8],
        to_read: usize,
    ) -> Result<(usize, usize), ArrowError> {
        self.skipping_records = true;
        let result = self.decode(input, to_read);
        self.skipping_records = false;
        result
    }

    pub(super) fn decode_validating_header(
        &mut self,
        input: &[u8],
    ) -> Result<(usize, usize), ArrowError> {
        let (rows, bytes) = if self.ignore_extra_fields {
            self.decode_strict::<true>(input, 1)?
        } else {
            self.decode_strict::<false>(input, 1)?
        };
        self.source_lines.advance(&input[..bytes]);
        self.stream_offset = self
            .stream_offset
            .checked_add(bytes)
            .ok_or_else(|| ArrowError::CsvError("CSV input byte offset overflowed".into()))?;
        if rows != 0 {
            self.record_byte_offset = self.stream_offset;
        }
        Ok((rows, bytes))
    }

    fn complete_recovery(
        &mut self,
        input: &[u8],
        counted_until: usize,
        consumed: usize,
        mut source_lines: SourceLineCounter,
        read: usize,
    ) -> (usize, usize) {
        source_lines.advance(&input[counted_until..consumed]);
        self.source_lines = source_lines;
        if self.record_bytes.is_empty() {
            self.record_needs_utf8_validation = false;
        }
        (read, consumed)
    }

    fn decode_strict<const IGNORE_EXTRA: bool>(
        &mut self,
        input: &[u8],
        to_read: usize,
    ) -> Result<(usize, usize), ArrowError> {
        if to_read == 0 {
            return Ok((0, 0));
        }
        if IGNORE_EXTRA && self.num_columns == 0 {
            return Err(ArrowError::CsvError(
                "CSV cannot ignore extra fields with an empty schema".to_owned(),
            ));
        }

        // Reserve sufficient capacity in offsets
        self.offsets
            .resize(self.offsets_len + to_read * self.num_columns, 0);

        // The current offset into `input`
        let mut input_offset = 0;
        let mut record_start = 0;

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
                let skipping_extra_fields = IGNORE_EXTRA && self.skipping_extra_fields;
                let (result, bytes_read, bytes_written, end_positions) = if skipping_extra_fields {
                    let mut skipped_data = [0_u8; 1024];
                    let mut skipped_offsets = [0_usize; 16];
                    self.delimiter.read_record(
                        &input[input_offset..],
                        &mut skipped_data,
                        &mut skipped_offsets,
                    )
                } else {
                    let offsets_end = if IGNORE_EXTRA {
                        self.record_offsets_start + self.num_columns
                    } else {
                        self.offsets.len()
                    };
                    self.delimiter.read_record(
                        &input[input_offset..],
                        &mut self.data[self.data_len..],
                        &mut self.offsets[self.offsets_len..offsets_end],
                    )
                };

                self.current_field += end_positions;
                input_offset += bytes_read;
                if skipping_extra_fields {
                    if self.quoted_empty.is_some() {
                        self.record_bytes
                            .extend_from_slice(&input[record_start..input_offset]);
                        record_start = input_offset;
                    }
                } else {
                    self.offsets_len += end_positions;
                    self.data_len += bytes_written;
                }

                match result {
                    ReadRecordResult::End | ReadRecordResult::InputEmpty => {
                        // Reached end of input
                        if self.quoted_empty.is_some() {
                            self.record_bytes
                                .extend_from_slice(&input[record_start..input_offset]);
                        }
                        return Ok((read, input_offset));
                    }
                    // Need to allocate more capacity
                    ReadRecordResult::OutputFull => {
                        if skipping_extra_fields {
                            continue;
                        }
                        break;
                    }
                    ReadRecordResult::OutputEndsFull => {
                        if skipping_extra_fields {
                            continue;
                        }
                        if IGNORE_EXTRA {
                            self.skipping_extra_fields = true;
                            self.data_len = self.record_data_start
                                + self.offsets[self.record_offsets_start + self.num_columns - 1];
                            self.offsets_len = self.record_offsets_start + self.num_columns;
                            if self.quoted_empty.is_some() {
                                self.record_bytes
                                    .extend_from_slice(&input[record_start..input_offset]);
                                record_start = input_offset;
                            }
                            continue;
                        }
                        return Err(ArrowError::CsvError(format!(
                            "incorrect number of fields for line {}, expected {} got more than {}",
                            self.line_number, self.num_columns, self.current_field
                        )));
                    }
                    ReadRecordResult::Record => {
                        self.skipping_extra_fields = false;
                        if self.current_field != self.num_columns
                            && !(IGNORE_EXTRA && self.current_field > self.num_columns)
                        {
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
                        if IGNORE_EXTRA && self.current_field > self.num_columns {
                            self.data_len = self.record_data_start
                                + self.offsets[self.record_offsets_start + self.num_columns - 1];
                            self.offsets_len = self.record_offsets_start + self.num_columns;
                        }
                        if let Some(counts) = &mut self.field_counts {
                            if counts.len() == self.num_rows {
                                counts.push(self.current_field);
                            } else {
                                counts[self.num_rows] = self.current_field;
                            }
                        }
                        if let Some(quoted) = &mut self.quoted_empty {
                            if self.record_bytes.is_empty() {
                                quoted.record(
                                    self.num_rows,
                                    self.num_columns,
                                    self.current_field,
                                    IGNORE_EXTRA,
                                    &input[record_start..input_offset],
                                )?;
                            } else {
                                self.record_bytes
                                    .extend_from_slice(&input[record_start..input_offset]);
                                quoted.record(
                                    self.num_rows,
                                    self.num_columns,
                                    self.current_field,
                                    IGNORE_EXTRA,
                                    &self.record_bytes,
                                )?;
                                self.record_bytes.clear();
                            }
                        }
                        record_start = input_offset;
                        read += 1;
                        self.current_field = 0;
                        self.line_number += 1;
                        self.num_rows += 1;
                        if IGNORE_EXTRA {
                            self.record_data_start = self.data_len;
                            self.record_offsets_start = self.offsets_len;
                        }

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
        if self.ignore_extra_fields && self.num_columns == 0 {
            return Err(ArrowError::CsvError(
                "CSV cannot ignore extra fields with an empty schema".to_owned(),
            ));
        }

        // The reader can pass the same unread tail again after a full batch. Bound
        // preflight work per requested row so small batches do not rescan that tail.
        const MAX_PREFLIGHT_BYTES_PER_ROW: usize = 64;
        let input_requires_utf8_validation = !self.utf8_preserving_syntax
            || input.len() > to_read.saturating_mul(MAX_PREFLIGHT_BYTES_PER_ROW)
            || std::str::from_utf8(input).is_err();
        self.record_needs_utf8_validation |= input_requires_utf8_validation;

        let required_offsets = to_read
            .checked_mul(self.num_columns)
            .and_then(|additional| self.offsets_len.checked_add(additional))
            .ok_or_else(|| ArrowError::CsvError("CSV field offset capacity overflowed".into()))?;
        self.offsets.resize(required_offsets, 0);
        let mut input_offset = 0;
        let mut read = 0;
        let mut counted_until = 0;
        let mut source_lines = self.source_lines;

        loop {
            let remaining_rows = to_read - read;
            let capacity = remaining_rows * self.num_columns * AVERAGE_FIELD_SIZE;
            let estimated_data = capacity.max(MIN_CAPACITY);
            self.data.resize(self.data_len + estimated_data, 0);

            loop {
                let skipping_extra_fields = self.ignore_extra_fields && self.skipping_extra_fields;
                let max_offsets_len = self
                    .record_offsets_start
                    .checked_add(self.num_columns)
                    .and_then(|expected| expected.checked_add(MAX_RECOVERABLE_EXCESS_FIELDS))
                    .ok_or_else(|| {
                        ArrowError::CsvError("CSV field offset capacity overflowed".into())
                    })?;
                let offsets_end = if self.ignore_extra_fields {
                    self.record_offsets_start + self.num_columns
                } else {
                    self.offsets.len().min(max_offsets_len)
                };
                let record_input_start = input_offset;
                let bom_stripped = self.stream_offset == self.initial_stream_offset
                    && input[record_input_start..].starts_with(b"\xef\xbb\xbf");
                let (result, bytes_read, bytes_written, end_positions) = if skipping_extra_fields {
                    let mut skipped_data = [0_u8; 1024];
                    let mut skipped_offsets = [0_usize; 16];
                    self.delimiter.read_record(
                        &input[input_offset..],
                        &mut skipped_data,
                        &mut skipped_offsets,
                    )
                } else {
                    self.delimiter.read_record(
                        &input[input_offset..],
                        &mut self.data[self.data_len..],
                        &mut self.offsets[self.offsets_len..offsets_end],
                    )
                };

                self.current_field += end_positions;
                if !skipping_extra_fields {
                    self.offsets_len += end_positions;
                    self.data_len += bytes_written;
                }
                input_offset += bytes_read;
                self.stream_offset =
                    self.stream_offset.checked_add(bytes_read).ok_or_else(|| {
                        ArrowError::CsvError("CSV input byte offset overflowed".into())
                    })?;
                if bom_stripped {
                    self.record_byte_offset = self.record_byte_offset.saturating_add(3);
                }
                if let Some(limit) = self.max_record_bytes
                    && self
                        .record_bytes
                        .len()
                        .saturating_add(input_offset.saturating_sub(record_input_start))
                        > limit
                {
                    return Err(ArrowError::CsvError(format!(
                        "CSV record exceeds the configured {limit} byte limit"
                    )));
                }
                self.record_bytes.extend_from_slice(
                    &input[record_input_start + if bom_stripped { 3 } else { 0 }..input_offset],
                );

                match result {
                    ReadRecordResult::End | ReadRecordResult::InputEmpty => {
                        return Ok(self.complete_recovery(
                            input,
                            counted_until,
                            input_offset,
                            source_lines,
                            read,
                        ));
                    }
                    ReadRecordResult::OutputFull => {
                        if skipping_extra_fields {
                            continue;
                        }
                        break;
                    }
                    ReadRecordResult::OutputEndsFull => {
                        if skipping_extra_fields {
                            continue;
                        }
                        if self.ignore_extra_fields {
                            self.skipping_extra_fields = true;
                            self.data_len = self.record_data_start
                                + self.offsets[self.record_offsets_start + self.num_columns - 1];
                            self.offsets_len = self.record_offsets_start + self.num_columns;
                            continue;
                        }
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
                        self.skipping_extra_fields = false;
                        let ignored_bytes = ignored_record_prefix(
                            &self.record_bytes,
                            self.comment,
                            self.terminator,
                        );
                        if ignored_bytes > 0 {
                            self.record_bytes.drain(..ignored_bytes);
                            self.record_byte_offset =
                                self.record_byte_offset.saturating_add(ignored_bytes);
                        }
                        if self.skipping_records {
                            self.skip_current_record();
                            self.record_needs_utf8_validation = input_requires_utf8_validation;
                            read += 1;
                            if read == to_read {
                                return Ok(self.complete_recovery(
                                    input,
                                    counted_until,
                                    input_offset,
                                    source_lines,
                                    read,
                                ));
                            }
                            continue;
                        }
                        if self.filter_records && handler.has_record_error(&self.record_bytes) {
                            source_lines.advance(&input[counted_until..input_offset]);
                            counted_until = input_offset;
                            if handler.skip_record(&CsvFilterRecord {
                                line_number: self.line_number,
                                source_line_number: source_lines.record_start_line(
                                    usize::try_from(self.delimiter.line()).unwrap_or(usize::MAX),
                                    &self.record_bytes,
                                ),
                                byte_offset: self.record_byte_offset,
                                record: &self.record_bytes,
                            })? {
                                self.skip_current_record();
                                self.record_needs_utf8_validation = input_requires_utf8_validation;
                                continue;
                            }
                        }
                        if self.current_field != self.num_columns
                            && !(self.truncated_rows && self.current_field < self.num_columns)
                            && !(self.ignore_extra_fields && self.current_field > self.num_columns)
                        {
                            source_lines.advance(&input[counted_until..input_offset]);
                            counted_until = input_offset;
                            handler.handle(&CsvRecordError {
                                line_number: self.line_number,
                                source_line_number: source_lines.record_start_line(
                                    usize::try_from(self.delimiter.line()).unwrap_or(usize::MAX),
                                    &self.record_bytes,
                                ),
                                byte_offset: self.record_byte_offset,
                                expected_fields: self.num_columns,
                                actual_fields: self.current_field,
                                record: &self.record_bytes,
                            })?;
                            self.skip_current_record();
                            self.record_needs_utf8_validation = input_requires_utf8_validation;
                            continue;
                        }

                        if self.ignore_extra_fields && self.current_field > self.num_columns {
                            self.data_len = self.record_data_start
                                + self.offsets[self.record_offsets_start + self.num_columns - 1];
                            self.offsets_len = self.record_offsets_start + self.num_columns;
                        }

                        let row_data = &self.data[self.record_data_start..self.data_len];
                        let invalid_field = (self.record_needs_utf8_validation
                            && !row_data.is_ascii())
                        .then(|| {
                            let field_ends =
                                &self.offsets[self.record_offsets_start..self.offsets_len];
                            match std::str::from_utf8(row_data) {
                                Ok(valid) => field_ends
                                    .iter()
                                    .position(|end| !valid.is_char_boundary(*end)),
                                Err(error) => Some(
                                    field_ends.partition_point(|end| *end <= error.valid_up_to()),
                                ),
                            }
                        })
                        .flatten();
                        if let Some(field_index) = invalid_field {
                            if self.replace_invalid_utf8 {
                                self.replace_invalid_utf8_fields()?;
                            } else {
                                source_lines.advance(&input[counted_until..input_offset]);
                                counted_until = input_offset;
                                handler.handle_invalid_utf8(&CsvInvalidUtf8Error {
                                    line_number: self.line_number,
                                    source_line_number: source_lines.record_start_line(
                                        usize::try_from(self.delimiter.line())
                                            .unwrap_or(usize::MAX),
                                        &self.record_bytes,
                                    ),
                                    byte_offset: self.record_byte_offset,
                                    field_index,
                                    record: &self.record_bytes,
                                })?;
                                self.skip_current_record();
                                self.record_needs_utf8_validation = input_requires_utf8_validation;
                                continue;
                            }
                        }

                        let record = CsvRecord {
                            line_number: self.line_number,
                            byte_offset: self.record_byte_offset,
                            record: &self.record_bytes,
                        };
                        if self.record_source_lines {
                            source_lines.advance(&input[counted_until..input_offset]);
                            counted_until = input_offset;
                            handler.handle_record_with_source_line(
                                &record,
                                source_lines.record_start_line(
                                    usize::try_from(self.delimiter.line()).unwrap_or(usize::MAX),
                                    &self.record_bytes,
                                ),
                            )?;
                        } else {
                            handler.handle_record(&record)?;
                        }

                        if self.current_field < self.num_columns {
                            let fill_count = self.num_columns - self.current_field;
                            let fill_value = self.offsets[self.offsets_len - 1];
                            self.offsets[self.offsets_len..self.offsets_len + fill_count]
                                .fill(fill_value);
                            self.offsets_len += fill_count;
                        }
                        if let Some(counts) = &mut self.field_counts {
                            if counts.len() == self.num_rows {
                                counts.push(self.current_field);
                            } else {
                                counts[self.num_rows] = self.current_field;
                            }
                        }
                        if let Some(quoted) = &mut self.quoted_empty {
                            quoted.record(
                                self.num_rows,
                                self.num_columns,
                                self.current_field,
                                self.ignore_extra_fields,
                                &self.record_bytes,
                            )?;
                        }
                        read += 1;
                        self.current_field = 0;
                        self.line_number += 1;
                        self.num_rows += 1;
                        self.record_bytes.clear();
                        self.record_needs_utf8_validation = input_requires_utf8_validation;
                        self.record_byte_offset = self.stream_offset;
                        self.record_data_start = self.data_len;
                        self.record_offsets_start = self.offsets_len;

                        if read == to_read {
                            return Ok(self.complete_recovery(
                                input,
                                counted_until,
                                input_offset,
                                source_lines,
                                read,
                            ));
                        }
                        if input.len() == input_offset {
                            return Ok(self.complete_recovery(
                                input,
                                counted_until,
                                input_offset,
                                source_lines,
                                read,
                            ));
                        }
                    }
                }
            }
        }
    }

    fn skip_current_record(&mut self) {
        self.skipping_extra_fields = false;
        self.data_len = self.record_data_start;
        self.offsets_len = self.record_offsets_start;
        self.current_field = 0;
        self.line_number += 1;
        self.record_bytes.clear();
        self.record_byte_offset = self.stream_offset;
    }

    fn replace_invalid_utf8_fields(&mut self) -> Result<(), ArrowError> {
        let row = self.data[self.record_data_start..self.data_len].to_vec();
        self.data.truncate(self.record_data_start);
        let mut field_start = 0;
        for end in &mut self.offsets[self.record_offsets_start..self.offsets_len] {
            let field_end = *end;
            let field = row.get(field_start..field_end).ok_or_else(|| {
                ArrowError::CsvError("CSV decoded field offset is invalid".to_owned())
            })?;
            self.data
                .extend_from_slice(String::from_utf8_lossy(field).as_bytes());
            *end = self.data.len() - self.record_data_start;
            field_start = field_end;
        }
        self.data_len = self.data.len();
        Ok(())
    }

    fn replace_invalid_utf8_batch(&mut self) -> Result<(), ArrowError> {
        let original = self.data[..self.data_len].to_vec();
        self.data.clear();
        let mut field_start = 0;
        for end in &mut self.offsets[1..self.offsets_len] {
            let field_end = *end;
            let field = original.get(field_start..field_end).ok_or_else(|| {
                ArrowError::CsvError("CSV decoded field offset is invalid".to_owned())
            })?;
            self.data
                .extend_from_slice(String::from_utf8_lossy(field).as_bytes());
            *end = self.data.len();
            field_start = field_end;
        }
        self.data_len = self.data.len();
        Ok(())
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

        if self.replace_invalid_utf8 && self.record_error_handler.is_none() {
            let decoded = std::str::from_utf8(&self.data[..self.data_len]);
            let invalid_field = match decoded {
                Ok(data) => self.offsets[1..self.offsets_len]
                    .iter()
                    .any(|end| !data.is_char_boundary(*end)),
                Err(_) => true,
            };
            if invalid_field {
                self.replace_invalid_utf8_batch()?;
            }
        }

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
        let field_counts = self.field_counts.as_ref().map(|counts| &counts[..num_rows]);
        let quoted_empty = self
            .quoted_empty
            .as_ref()
            .map(|quoted| &quoted.fields[..self.offsets_len - 1]);

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
            field_counts,
            quoted_empty,
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
    field_counts: Option<&'a [usize]>,
    quoted_empty: Option<&'a [u8]>,
    data: &'a str,
}

impl<'a> StringRecords<'a> {
    fn get(&self, index: usize) -> StringRecord<'a> {
        let field_idx = index * self.num_columns;
        StringRecord {
            data: self.data,
            offsets: &self.offsets[field_idx..field_idx + self.num_columns + 1],
            field_count: self.field_counts.map(|counts| counts[index]),
            quoted_empty: self
                .quoted_empty
                .map(|flags| &flags[field_idx..field_idx + self.num_columns]),
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
    field_count: Option<usize>,
    quoted_empty: Option<&'a [u8]>,
}

impl<'a> StringRecord<'a> {
    #[inline]
    pub fn is_missing(&self, index: usize) -> bool {
        self.field_count.is_some_and(|count| index >= count)
    }

    #[inline]
    pub(super) fn is_null(&self, index: usize, value: &str, null_regex: &NullRegex) -> bool {
        self.is_missing(index)
            || (!self.quoted_empty.is_some_and(|flags| flags[index] != 0)
                && null_regex.is_null(value))
    }

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
    use crate::reader::{
        CsvFilterRecord, CsvInvalidUtf8Error, CsvRecord, CsvRecordError, CsvRecordErrorHandler,
    };
    use arrow_schema::ArrowError;
    use csv_core::Reader;
    use std::io::{BufRead, BufReader, Cursor};
    use std::sync::{Arc, Mutex};
    use std::time::Instant;

    use super::{MAX_RECOVERABLE_EXCESS_FIELDS, RecordDecoder, recovery_limit_error};

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct OwnedRecordError {
        line_number: usize,
        source_line_number: usize,
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

    #[derive(Debug, Clone, Eq, PartialEq)]
    struct OwnedInvalidUtf8Error {
        line_number: usize,
        source_line_number: usize,
        byte_offset: usize,
        field_index: usize,
        record: Vec<u8>,
    }

    #[derive(Debug, Default)]
    struct CollectRecords {
        errors: Mutex<Vec<OwnedRecordError>>,
        invalid_utf8: Mutex<Vec<OwnedInvalidUtf8Error>>,
        records: Mutex<Vec<OwnedRecord>>,
    }

    #[derive(Debug, Default)]
    struct SourceLines(Mutex<Vec<(usize, usize, usize)>>);

    impl CsvRecordErrorHandler for SourceLines {
        fn handle(&self, _error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
            Ok(())
        }

        fn needs_source_line_number(&self) -> bool {
            true
        }

        fn handle_record_with_source_line(
            &self,
            record: &CsvRecord<'_>,
            source_line_number: usize,
        ) -> Result<(), ArrowError> {
            self.0.lock().unwrap().push((
                record.line_number,
                source_line_number,
                record.byte_offset,
            ));
            Ok(())
        }
    }

    impl CsvRecordErrorHandler for CollectRecords {
        fn handle(&self, error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
            self.errors.lock().unwrap().push(OwnedRecordError {
                line_number: error.line_number,
                source_line_number: error.source_line_number,
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

        fn handle_invalid_utf8(&self, error: &CsvInvalidUtf8Error<'_>) -> Result<(), ArrowError> {
            self.invalid_utf8
                .lock()
                .unwrap()
                .push(OwnedInvalidUtf8Error {
                    line_number: error.line_number,
                    source_line_number: error.source_line_number,
                    byte_offset: error.byte_offset,
                    field_index: error.field_index,
                    record: error.record.to_vec(),
                });
            Ok(())
        }
    }

    #[derive(Debug, Default)]
    struct SkipSecondRecord {
        skipped: Mutex<Vec<(usize, usize, usize)>>,
    }

    impl CsvRecordErrorHandler for SkipSecondRecord {
        fn handle(&self, _error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
            Ok(())
        }

        fn filters_records(&self) -> bool {
            true
        }

        fn has_record_error(&self, record: &[u8]) -> bool {
            record.starts_with(b"2,")
        }

        fn skip_record(&self, record: &CsvFilterRecord<'_>) -> Result<bool, ArrowError> {
            if record.record.starts_with(b"2,") {
                self.skipped.lock().unwrap().push((
                    record.line_number,
                    record.source_line_number,
                    record.byte_offset,
                ));
                return Ok(true);
            }
            Ok(false)
        }
    }

    #[test]
    fn test_record_filter_skips_one_row_and_tracks_physical_line_across_chunks() {
        let csv = b"1,\"two\nlines\"\n2,bad\n3,after\n";
        let handler = Arc::new(SkipSecondRecord::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let mut reader = BufReader::with_capacity(2, Cursor::new(csv));

        loop {
            let buf = reader.fill_buf().unwrap();
            let (_, bytes) = decoder.decode(buf, 3 - decoder.len()).unwrap();
            reader.consume(bytes);
            if bytes == 0 || decoder.len() == 3 {
                break;
            }
        }

        let rows = decoder
            .flush()
            .unwrap()
            .iter()
            .map(|record| [record.get(0).to_owned(), record.get(1).to_owned()])
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                ["1".to_owned(), "two\nlines".to_owned()],
                ["3".to_owned(), "after".to_owned()]
            ]
        );
        assert_eq!(*handler.skipped.lock().unwrap(), [(2, 3, 14)]);
    }

    #[derive(Debug)]
    struct LimitedRecord;

    impl CsvRecordErrorHandler for LimitedRecord {
        fn handle(&self, _error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
            Ok(())
        }

        fn max_record_bytes(&self) -> Option<usize> {
            Some(8)
        }
    }

    #[test]
    fn test_recovery_record_limit_bounds_unterminated_quote() {
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(Arc::new(LimitedRecord)));
        let error = decoder.decode(b"1,\"unterminated", 1).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("CSV record exceeds the configured 8 byte limit"),
            "{error}"
        );
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
    fn test_ignore_large_extra_field_without_decoding_it() {
        let long_extra = "x".repeat(1_000_000);
        let csv = format!("kept,{long_extra}\nnext,extra\n");
        let mut decoder =
            RecordDecoder::new(Reader::new(), 1, false).with_ignore_extra_fields(true);
        let (rows, bytes) = decoder.decode(csv.as_bytes(), 1024).unwrap();
        assert_eq!((rows, bytes), (2, csv.len()));
        assert_eq!(decoder.data_len, "keptnext".len());
        assert!(decoder.data.len() < 64 * 1024);
        let records = decoder.flush().unwrap();
        assert_eq!(
            records.iter().map(|row| row.get(0)).collect::<Vec<_>>(),
            ["kept", "next"]
        );
    }

    #[test]
    fn test_ignore_more_than_recovery_limit_extra_fields() {
        let csv = format!(
            "kept{}\n",
            ",ignored".repeat(MAX_RECOVERABLE_EXCESS_FIELDS + 1)
        );
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 1, false)
            .with_ignore_extra_fields(true)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv.as_bytes(), 1024).unwrap();
        assert_eq!((rows, bytes), (1, csv.len()));
        assert_eq!(decoder.data_len, "kept".len());
        let records = decoder.flush().unwrap();
        assert_eq!(records.get(0).get(0), "kept");
        assert!(handler.errors.lock().unwrap().is_empty());
        assert_eq!(handler.records.lock().unwrap().len(), 1);
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
                    source_line_number: 2,
                    byte_offset: 5,
                    expected_fields: 2,
                    actual_fields: 3,
                    record: b"2,extra,value\n".to_vec(),
                },
                OwnedRecordError {
                    line_number: 3,
                    source_line_number: 3,
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
    fn test_record_error_reports_physical_line_after_ignored_input() {
        for (csv, expected_line) in [
            (b"1,ok\n\n2,extra,value\n".as_slice(), 3),
            (
                b"\"header\ncontinued\",value\n2,extra,value\n".as_slice(),
                3,
            ),
        ] {
            let handler = Arc::new(CollectRecords::default());
            let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
                .with_record_error_handler(Some(handler.clone()));
            let (read, consumed) = decoder.decode(csv, 1).expect("first record");
            assert_eq!(read, 1);
            decoder.clear();
            decoder
                .decode(&csv[consumed..], 1)
                .expect("recover malformed record");

            let errors = handler.errors.lock().unwrap();
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].line_number, 2);
            assert_eq!(errors[0].source_line_number, expected_line);
        }
    }

    #[test]
    fn test_successful_record_source_lines_after_skipped_prefix() {
        let csv = b"\"value\ncontinued\",ok\n2,good\n";
        let handler = Arc::new(SourceLines::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_initial_source_position(1, 7)
            .with_record_error_handler(Some(handler.clone()));
        assert_eq!(decoder.decode(csv, 2).unwrap(), (2, csv.len()));
        assert_eq!(
            *handler.0.lock().unwrap(),
            [(1, 2, 7), (2, 4, 7 + b"\"value\ncontinued\",ok\n".len())]
        );
    }

    #[test]
    fn test_successful_record_source_line_after_blank_line() {
        let csv = b"1,ok\n\nbad,ok\n";
        let handler = Arc::new(SourceLines::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 2).unwrap();
        assert_eq!((rows, bytes), (2, csv.len()));
        assert_eq!(*handler.0.lock().unwrap(), [(1, 1, 0), (2, 3, 6)]);
    }

    #[test]
    fn test_successful_record_source_lines_split_crlf_custom_terminator() {
        let csv = b"1,a\r\nb|2,ok|";
        let mut builder = csv_core::ReaderBuilder::new();
        builder.terminator(csv_core::Terminator::Any(b'|'));
        let handler = Arc::new(SourceLines::default());
        let mut decoder = RecordDecoder::new(builder.build(), 2, false)
            .with_ignored_prefix_options(None, Some(b'|'))
            .with_record_error_handler(Some(handler.clone()));
        let mut reader = BufReader::with_capacity(1, Cursor::new(csv));
        while decoder.len() < 2 {
            let buf = reader.fill_buf().unwrap();
            let (_, consumed) = decoder.decode(buf, 2 - decoder.len()).unwrap();
            reader.consume(consumed);
            if consumed == 0 {
                break;
            }
        }
        assert_eq!(decoder.len(), 2);
        assert_eq!(*handler.0.lock().unwrap(), [(1, 1, 0), (2, 2, 7)]);
    }

    #[test]
    fn test_invalid_utf8_handler_skips_only_malformed_record_across_chunks() {
        let csv = b"1,ok\n2,\xff\n3,after\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let mut reader = BufReader::with_capacity(2, Cursor::new(csv));

        loop {
            let buf = reader.fill_buf().unwrap();
            let (_, bytes) = decoder.decode(buf, 3 - decoder.len()).unwrap();
            reader.consume(bytes);
            if bytes == 0 || decoder.len() == 3 {
                break;
            }
        }

        let rows = decoder
            .flush()
            .unwrap()
            .iter()
            .map(|record| [record.get(0).to_owned(), record.get(1).to_owned()])
            .collect::<Vec<_>>();
        assert_eq!(
            rows,
            [
                [String::from("1"), String::from("ok")],
                [String::from("3"), String::from("after")],
            ]
        );
        assert_eq!(
            *handler.invalid_utf8.lock().unwrap(),
            [OwnedInvalidUtf8Error {
                line_number: 2,
                source_line_number: 2,
                byte_offset: 5,
                field_index: 1,
                record: b"2,\xff\n".to_vec(),
            }]
        );
        assert_eq!(handler.records.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_invalid_utf8_handler_default_still_errors() {
        #[derive(Debug)]
        struct FieldCountOnly;

        impl CsvRecordErrorHandler for FieldCountOnly {
            fn handle(&self, _error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
                Ok(())
            }
        }

        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(Arc::new(FieldCountOnly)));
        let error = decoder.decode(b"1,\xff\n", 1).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Csv error: Encountered invalid UTF-8 data for line 1 and field 2"
        );
    }

    #[test]
    fn test_invalid_utf8_cannot_cross_field_boundary() {
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(b"\xc3,\xa9\n1,ok\n", 2).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(bytes, 9);
        let records = decoder.flush().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records.get(0).get(0), "1");
        assert_eq!(
            *handler.invalid_utf8.lock().unwrap(),
            [OwnedInvalidUtf8Error {
                line_number: 1,
                source_line_number: 1,
                byte_offset: 0,
                field_index: 0,
                record: b"\xc3,\xa9\n".to_vec(),
            }]
        );
    }

    #[test]
    fn test_unicode_after_first_record_uses_relative_field_offsets() {
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let csv = "1,ok\n2,é\n".as_bytes();
        let (rows, bytes) = decoder.decode(csv, 2).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(bytes, csv.len());
        let records = decoder.flush().unwrap();
        assert_eq!(records.get(1).get(1), "é");
        assert!(handler.invalid_utf8.lock().unwrap().is_empty());
    }

    #[test]
    fn test_invalid_utf8_skipped_header_does_not_consume_data_row() {
        let csv = b"bad,\xff\n1,ok\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (skipped, header_bytes) = decoder.decode_skipping(csv, 1).unwrap();
        assert_eq!(skipped, 1);
        assert_eq!(header_bytes, 6);
        decoder.clear();

        let (rows, data_bytes) = decoder.decode(&csv[header_bytes..], 1).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(header_bytes + data_bytes, csv.len());
        let records = decoder.flush().unwrap();
        assert_eq!(records.get(0).get(0), "1");
        assert_eq!(records.get(0).get(1), "ok");
        assert!(handler.invalid_utf8.lock().unwrap().is_empty());
    }

    #[test]
    fn test_invalid_utf8_after_comment_reports_only_data_record() {
        let csv = b"1,ok\n#\xff ignored\n2,\xff\n3,after\n";
        let mut builder = csv_core::ReaderBuilder::new();
        builder.comment(Some(b'#'));
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(builder.build(), 2, false)
            .with_ignored_prefix_options(Some(b'#'), None)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 3).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 3);
        assert_eq!(errors[0].record, b"2,\xff\n");
        assert_eq!(errors[0].byte_offset, 16);
    }

    #[test]
    fn test_invalid_utf8_after_blank_line_reports_data_position() {
        let csv = b"1,ok\n\n2,\xff\n3,after\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 3).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 3);
        assert_eq!(errors[0].record, b"2,\xff\n");
        assert_eq!(errors[0].byte_offset, 6);
    }

    #[test]
    fn test_initial_bom_is_excluded_from_comment_and_record_diagnostics() {
        let csv = b"\xef\xbb\xbf#\xff\n2,\xfe\n";
        let mut builder = csv_core::ReaderBuilder::new();
        builder.comment(Some(b'#'));
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(builder.build(), 2, false)
            .with_ignored_prefix_options(Some(b'#'), None)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 1).unwrap();
        assert_eq!(rows, 0);
        assert_eq!(bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 1);
        assert_eq!(errors[0].source_line_number, 2);
        assert_eq!(errors[0].byte_offset, 6);
        assert_eq!(errors[0].field_index, 1);
        assert_eq!(errors[0].record, b"2,\xfe\n");
    }

    #[test]
    fn test_partial_initial_bom_is_not_silently_stripped() {
        let csv = b"\xef\xbb\xbf#\xff\n2,\xfe\n";
        let mut builder = csv_core::ReaderBuilder::new();
        builder.comment(Some(b'#'));
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(builder.build(), 2, false)
            .with_ignored_prefix_options(Some(b'#'), None)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(&csv[..2], 1).unwrap();
        assert_eq!((rows, bytes), (0, 2));
        let (rows, bytes) = decoder.decode(&csv[2..], 1).unwrap();
        assert_eq!(rows, 0);
        assert_eq!(bytes, csv.len() - 2);
        let field_errors = handler.errors.lock().unwrap();
        assert_eq!(field_errors.len(), 1);
        assert_eq!(field_errors[0].record, b"\xef\xbb\xbf#\xff\n");
        let utf8_errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(utf8_errors.len(), 1);
        assert_eq!(utf8_errors[0].record, b"2,\xfe\n");
        assert_eq!(utf8_errors[0].byte_offset, 6);
    }

    #[test]
    fn test_invalid_utf8_after_multiline_record_keeps_record_and_source_lines_distinct() {
        let csv = b"1,\"two\nlines\"\n2,\xff\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 2).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 3);
        assert_eq!(errors[0].byte_offset, 14);
    }

    #[test]
    fn test_invalid_utf8_inside_multiline_record_skips_whole_record() {
        let csv = b"1,ok\n2,\"before\n\xff\"\n3,after\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_ignored_prefix_options(None, None)
            .with_utf8_preserving_syntax(true)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 3).unwrap();
        assert_eq!(bytes, csv.len());
        assert_eq!(rows, 2);
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 2);
        assert_eq!(errors[0].record, b"2,\"before\n\xff\"\n");
    }

    #[test]
    fn test_replace_invalid_utf8_preserves_records_and_field_offsets() {
        let csv = b"1,ok\n2,\xff\xfe,extra\n3,after\n";
        let mut decoder =
            RecordDecoder::new(Reader::new(), 3, true).with_replace_invalid_utf8(true);
        let (rows, bytes) = decoder.decode(csv, 3).unwrap();
        assert_eq!((rows, bytes), (3, csv.len()));
        let records = decoder.flush().unwrap();
        let values = records
            .iter()
            .map(|record| (record.get(0), record.get(1), record.get(2)))
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            [("1", "ok", ""), ("2", "��", "extra"), ("3", "after", "")]
        );
    }

    #[test]
    fn test_replace_invalid_utf8_observes_repaired_record_without_error() {
        let csv = b"1,\"bef\xffore\"\n2,after\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_replace_invalid_utf8(true)
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 2).unwrap();
        assert_eq!((rows, bytes), (2, csv.len()));
        let records = decoder.flush().unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.get(1))
                .collect::<Vec<_>>(),
            ["bef�ore", "after"]
        );
        assert!(handler.invalid_utf8.lock().unwrap().is_empty());
        assert_eq!(handler.records.lock().unwrap().len(), 2);
    }

    #[test]
    fn test_replace_invalid_utf8_does_not_replace_valid_split_unicode() {
        let csv = "1,é\n".as_bytes();
        let mut decoder =
            RecordDecoder::new(Reader::new(), 2, false).with_replace_invalid_utf8(true);
        let (first_rows, first_bytes) = decoder.decode(&csv[..3], 1).unwrap();
        let (later_rows, later_bytes) = decoder.decode(&csv[3..], 1).unwrap();
        assert_eq!(first_rows + later_rows, 1);
        assert_eq!(first_bytes + later_bytes, csv.len());
        let records = decoder.flush().unwrap();
        assert_eq!(records.iter().next().unwrap().get(1), "é");
    }

    #[test]
    fn test_replace_invalid_utf8_still_rejects_field_count_mismatch() {
        let mut decoder =
            RecordDecoder::new(Reader::new(), 2, false).with_replace_invalid_utf8(true);
        let error = decoder.decode(b"1,ok,extra\n", 1).unwrap_err();
        assert!(error.to_string().contains("incorrect number of fields"));
    }

    #[test]
    fn test_replace_invalid_utf8_split_across_field_boundary() {
        let mut decoder = RecordDecoder::new(Reader::new(), 3, false)
            .with_utf8_preserving_syntax(true)
            .with_replace_invalid_utf8(true);
        let csv = b"1,\xc3,\xa9\n";
        assert_eq!(decoder.decode(csv, 1).unwrap(), (1, csv.len()));
        let records = decoder.flush().unwrap();
        let record = records.iter().next().unwrap();
        assert_eq!(
            (record.get(0), record.get(1), record.get(2)),
            ("1", "�", "�")
        );
    }

    #[test]
    fn test_replace_invalid_utf8_after_high_byte_quote() {
        let mut builder = csv_core::ReaderBuilder::new();
        builder.quote(b'\xc3');
        let mut decoder =
            RecordDecoder::new(builder.build(), 2, false).with_replace_invalid_utf8(true);
        let csv = b"1,\xc3\xa9\xc3\xa9\n";
        assert_eq!(decoder.decode(csv, 1).unwrap(), (1, csv.len()));
        let records = decoder.flush().unwrap();
        assert_eq!(records.iter().next().unwrap().get(1), "��");
    }

    #[test]
    fn test_validated_header_preserves_later_recovery_offsets() {
        let csv = b"id,msg\n2,\xff\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (header_rows, header_bytes) = decoder.decode_validating_header(csv).unwrap();
        assert_eq!((header_rows, header_bytes), (1, 7));
        decoder.flush().unwrap();
        let (rows, bytes) = decoder.decode(&csv[header_bytes..], 1).unwrap();
        assert_eq!(rows, 0);
        assert_eq!(header_bytes + bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 2);
        assert_eq!(errors[0].byte_offset, 7);
    }

    #[test]
    fn test_source_lines_across_split_crlf_and_multiple_errors() {
        let csv = b"1,ok\r\n2,\xff\r\n3,\xfe\r\n4,after\r\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
            .with_record_error_handler(Some(handler.clone()));
        let (first_rows, first_bytes) = decoder.decode(&csv[..5], 4).unwrap();
        assert_eq!(first_bytes, 5);
        let (later_rows, later_bytes) = decoder.decode(&csv[5..], 4).unwrap();
        assert_eq!(first_rows + later_rows, 2);
        assert_eq!(first_bytes + later_bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 2);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 2);
        assert_eq!(errors[0].byte_offset, 6);
        assert_eq!(errors[1].line_number, 3);
        assert_eq!(errors[1].source_line_number, 3);
        assert_eq!(errors[1].byte_offset, 11);
    }

    #[test]
    fn test_source_lines_with_bare_cr_across_input_chunks() {
        let csv = b"1,ok\r2,\xff\r3,after\r";
        let mut builder = csv_core::ReaderBuilder::new();
        builder.terminator(csv_core::Terminator::Any(b'\r'));
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(builder.build(), 2, false)
            .with_ignored_prefix_options(None, Some(b'\r'))
            .with_record_error_handler(Some(handler.clone()));
        let (first_rows, first_bytes) = decoder.decode(&csv[..5], 3).unwrap();
        let (later_rows, later_bytes) = decoder.decode(&csv[5..], 3).unwrap();
        assert_eq!(first_rows + later_rows, 2);
        assert_eq!(first_bytes + later_bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 2);
        assert_eq!(errors[0].byte_offset, 5);
    }

    #[test]
    fn test_custom_terminator_counts_newlines_inside_fields() {
        let csv = b"1,a\nb|2,\xff|";
        let mut builder = csv_core::ReaderBuilder::new();
        builder.terminator(csv_core::Terminator::Any(b'|'));
        let handler = Arc::new(CollectRecords::default());
        let mut decoder = RecordDecoder::new(builder.build(), 2, false)
            .with_ignored_prefix_options(None, Some(b'|'))
            .with_record_error_handler(Some(handler.clone()));
        let (rows, bytes) = decoder.decode(csv, 2).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(bytes, csv.len());
        let errors = handler.invalid_utf8.lock().unwrap();
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].line_number, 2);
        assert_eq!(errors[0].source_line_number, 2);
        assert_eq!(errors[0].byte_offset, 6);
    }

    #[test]
    #[ignore = "local CSV record-handler throughput benchmark"]
    fn bench_valid_csv_with_record_handler() {
        bench_valid_csv_row(b"1,ok\n", false);
    }

    #[test]
    #[ignore = "local CSV source-line callback throughput benchmark"]
    fn bench_valid_csv_with_source_line_handler() {
        bench_valid_csv_row(b"1,ok\n", true);
    }

    #[test]
    #[ignore = "local CSV record-handler throughput benchmark"]
    fn bench_valid_unicode_csv_with_record_handler() {
        bench_valid_csv_row("1,é\n".as_bytes(), false);
    }

    #[test]
    #[ignore = "local CSV record-handler throughput benchmark"]
    fn bench_valid_quoted_csv_with_record_handler() {
        bench_valid_csv_row(b"1,\"ok\"\n", false);
    }

    #[test]
    #[ignore = "local CSV record-filter throughput benchmark"]
    fn bench_valid_quoted_csv_with_record_filter() {
        #[derive(Debug)]
        struct QuotedRecordFilter;

        impl CsvRecordErrorHandler for QuotedRecordFilter {
            fn handle(&self, _error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
                Ok(())
            }

            fn filters_records(&self) -> bool {
                true
            }

            fn has_record_error(&self, record: &[u8]) -> bool {
                std::hint::black_box(record.contains(&b'"'));
                false
            }
        }

        const ROWS: usize = 100_000;
        let csv = b"1,\"ok\"\n".repeat(ROWS);
        let handler = Arc::new(QuotedRecordFilter);
        let mut elapsed = Vec::new();
        for _ in 0..6 {
            let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
                .with_ignored_prefix_options(None, None)
                .with_utf8_preserving_syntax(true)
                .with_record_error_handler(Some(handler.clone()));
            let started = Instant::now();
            let (rows, bytes) = decoder.decode(&csv, ROWS).unwrap();
            let records = decoder.flush().unwrap();
            assert_eq!(rows, ROWS);
            assert_eq!(bytes, csv.len());
            assert_eq!(records.len(), ROWS);
            elapsed.push(started.elapsed());
        }
        println!("CSV quoted record filter times: {elapsed:?}");
    }

    fn bench_valid_csv_row(row: &[u8], source_lines: bool) {
        #[derive(Debug)]
        struct Noop {
            source_lines: bool,
        }

        impl CsvRecordErrorHandler for Noop {
            fn handle(&self, _error: &CsvRecordError<'_>) -> Result<(), ArrowError> {
                Ok(())
            }

            fn needs_source_line_number(&self) -> bool {
                self.source_lines
            }

            fn handle_record_with_source_line(
                &self,
                _record: &CsvRecord<'_>,
                source_line_number: usize,
            ) -> Result<(), ArrowError> {
                std::hint::black_box(source_line_number);
                Ok(())
            }
        }

        const ROWS: usize = 100_000;
        let csv = row.repeat(ROWS);
        let handler = Arc::new(Noop { source_lines });
        let mut elapsed = Vec::new();
        for _ in 0..6 {
            let mut decoder = RecordDecoder::new(Reader::new(), 2, false)
                .with_ignored_prefix_options(None, None)
                .with_utf8_preserving_syntax(true)
                .with_record_error_handler(Some(handler.clone()));
            let started = Instant::now();
            let (rows, bytes) = decoder.decode(&csv, ROWS).unwrap();
            let records = decoder.flush().unwrap();
            assert_eq!(rows, ROWS);
            assert_eq!(bytes, csv.len());
            assert_eq!(records.len(), ROWS);
            elapsed.push(started.elapsed());
        }
        println!("CSV record handler (source_lines={source_lines}) times: {elapsed:?}");
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

        let records = decoder.flush().unwrap();
        let missing = records
            .iter()
            .map(|record| record.is_missing(1))
            .collect::<Vec<_>>();
        assert_eq!(missing, [false, true, false, false, false]);
        assert_eq!(records.get(1).get(1), "");
        assert_eq!(records.get(2).get(0), "");
    }

    #[test]
    fn test_truncated_rows_with_error_handler() {
        let csv = b"1,ok\n2,extra,value\n3\n4,after\n";
        let handler = Arc::new(CollectRecords::default());
        let mut decoder =
            RecordDecoder::new(Reader::new(), 2, true).with_record_error_handler(Some(handler));
        let (read, bytes) = decoder.decode(csv, 2).unwrap();
        assert_eq!(read, 2);

        let records = decoder.flush().unwrap();
        let values = records
            .iter()
            .map(|record| (record.get(0).to_owned(), record.is_missing(1)))
            .collect::<Vec<_>>();
        assert_eq!(values, [("1".into(), false), ("3".into(), true)]);

        let (read, remaining_bytes) = decoder.decode(&csv[bytes..], 2).unwrap();
        assert_eq!(read, 1);
        assert_eq!(bytes + remaining_bytes, csv.len());
        let records = decoder.flush().unwrap();
        assert_eq!(records.get(0).get(0), "4");
        assert!(!records.get(0).is_missing(1));
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
