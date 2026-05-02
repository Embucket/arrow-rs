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

//! Approximate distinct-value counting via the [HyperLogLog] algorithm.
//!
//! [HyperLogLog]: https://en.wikipedia.org/wiki/HyperLogLog

use arrow_array::{
    ArrayRef, PrimitiveArray, UInt8Array,
    cast::as_primitive_array,
    types::{Int64Type, UInt8Type, UInt64Type},
};
use twox_hash::XxHash64;

use crate::errors::ParquetError;

const HLL_HASH_SEED: u64 = 0;

/// HyperLogLog sketch with `m = 256` registers (precision `p = 8`).
///
/// Each `Int64` value inserted is hashed with xxHash64; the top 8 bits select
/// the register and the remaining 56 bits contribute their *rank* — the
/// 1-indexed position of the leftmost 1-bit, i.e. `leading_zeros + 1`. Each
/// register tracks the maximum rank observed for its bucket, and
/// [`count`](Self::count) derives a cardinality estimate from those values.
pub struct HyperLogLog {
    registers: [u8; 256],
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    /// Create an empty sketch with all registers set to zero.
    pub fn new() -> Self {
        Self {
            registers: [0; 256],
        }
    }

    /// Insert every non-null value of `array` (must be `Int64`) into the sketch.
    ///
    /// Each value is hashed once; for every register `i` the new maximum
    /// rank among hashes routed to bucket `i` is merged into `registers[i]`.
    /// Returns an error if `array` is not an `Int64Array` or if the underlying
    /// Arrow compute kernels fail.
    pub fn insert_array(&mut self, array: ArrayRef) -> Result<(), ParquetError> {
        let array: &PrimitiveArray<Int64Type> = as_primitive_array(&array);
        let hashes: PrimitiveArray<UInt64Type> =
            array.unary(|v| XxHash64::oneshot(HLL_HASH_SEED, &v.to_le_bytes()));
        let indices: UInt8Array = hashes.unary(|h| (h >> 56) as u8);
        let ranks: UInt8Array = hashes.unary(|h| (h << 8).leading_zeros() as u8 + 1);

        for i in 0..=255u8 {
            let scalar = UInt8Array::new_scalar(i);
            let mask = arrow_ord::cmp::eq(&indices, &scalar)?;
            let filtered = arrow_select::filter::filter(&ranks, &mask)?;
            let filtered: &PrimitiveArray<UInt8Type> = as_primitive_array(&filtered);
            if let Some(max_rank) = arrow_arith::aggregate::max(filtered) {
                let register = &mut self.registers[i as usize];
                if max_rank > *register {
                    *register = max_rank;
                }
            }
        }
        Ok(())
    }

    /// Estimate the number of distinct values inserted so far.
    ///
    /// Uses the standard HyperLogLog estimator
    /// `E = α_m · m² / Σ 2^(-M[j])` (harmonic-mean form) with the
    /// bias-correction constant `α_m` for `m = 256`. When the raw estimate is
    /// small (`≤ 2.5 m`) and at least one register is still zero, falls back to
    /// linear counting `m · ln(m / V)`, which is more accurate at low
    /// cardinalities. See Flajolet et al., *HyperLogLog: the analysis of a
    /// near-optimal cardinality estimation algorithm* (2007).
    pub fn count(&self) -> f64 {
        const M: usize = 256;
        const M_F: f64 = M as f64;
        const ALPHA: f64 = 0.7213 / (1.0 + 1.079 / M_F);

        let z: f64 = self.registers.iter().map(|&r| (-(r as f64)).exp2()).sum();
        let raw = ALPHA * M_F * M_F / z;

        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        if raw <= 2.5 * M_F && zeros > 0 {
            M_F * (M_F / zeros as f64).ln()
        } else {
            raw
        }
    }
}
