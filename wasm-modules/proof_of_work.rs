use sha2::{Sha256, Digest};
use std::slice;

#[no_mangle]
pub extern "C" fn double_sha256(input_ptr: *const u8, input_len: usize, output_ptr: *mut u8) {
    // SAFETY: The caller must guarantee the pointers are valid
    let input = unsafe { slice::from_raw_parts(input_ptr, input_len) };
    let first_hash = Sha256::digest(input);
    let second_hash = Sha256::digest(&first_hash);

    // Copy the result into the output buffer
    unsafe {
        std::ptr::copy_nonoverlapping(second_hash.as_ptr(), output_ptr, 32);
    }
}