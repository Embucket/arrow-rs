use arrow_array::{
    ArrayRef, PrimitiveArray,
    cast::as_primitive_array,
    types::{Int64Type, UInt16Type, UInt64Type},
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
        let packed: PrimitiveArray<UInt16Type> = hashes.unary(|h| {
            let index = (h >> 56) as u16;
            let lz = (h << 8).leading_zeros() as u16;
            (index << 8) | lz
        });
        for entry in packed.iter().flatten() {
            let [index, lz] = entry.to_be_bytes();
            let register = &mut self.registers[index as usize];
            if lz > *register {
                *register = lz;
            }
        }
        Ok(())
    }
}
