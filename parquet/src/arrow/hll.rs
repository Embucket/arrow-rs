use arrow_array::{
    ArrayRef, PrimitiveArray, UInt8Array,
    cast::as_primitive_array,
    types::{Int64Type, UInt8Type, UInt64Type},
};
use twox_hash::XxHash64;

use crate::errors::ParquetError;

const HLL_HASH_SEED: u64 = 0;

struct HyperLogLog {
    registers: [u8; 256],
}

impl HyperLogLog {
    pub fn insert_array(&mut self, array: ArrayRef) -> Result<(), ParquetError> {
        let array: &PrimitiveArray<Int64Type> = as_primitive_array(&array);
        let hashes: PrimitiveArray<UInt64Type> =
            array.unary(|v| XxHash64::oneshot(HLL_HASH_SEED, &v.to_le_bytes()));
        let indices: UInt8Array = hashes.unary(|h| (h >> 56) as u8);
        let leading_zeros: UInt8Array = hashes.unary(|h| (h << 8).leading_zeros() as u8);

        for i in 0..=255u8 {
            let scalar = UInt8Array::new_scalar(i);
            let mask = arrow_ord::cmp::eq(&indices, &scalar)?;
            let filtered = arrow_select::filter::filter(&leading_zeros, &mask)?;
            let filtered: &PrimitiveArray<UInt8Type> = as_primitive_array(&filtered);
            if let Some(max_lz) = arrow_arith::aggregate::max(filtered) {
                let register = &mut self.registers[i as usize];
                if max_lz > *register {
                    *register = max_lz;
                }
            }
        }
        Ok(())
    }
}
