"""
Hash function for binary_name that matches the Rust implementation.
Use this in your Python training code to ensure consistency.

Usage in Python:
    from hash_binary_name import hash_binary_name
    
    # In your preprocessing:
    features.iloc[:,0] = features.iloc[:,0].apply(lambda x: hash_binary_name(x))
"""

def hash_binary_name(s: str) -> int:
    """
    Hash function that matches the Rust implementation exactly.
    This produces the same hash value as the Rust hash_binary_name function.
    
    Use this in your training code instead of abs(hash(x)):
        features.iloc[:,0] = features.iloc[:,0].apply(lambda x: hash_binary_name(x))
    
    Args:
        s: The string to hash (binary_name)
    
    Returns:
        A positive integer hash value (will be cast to f32 in Rust)
    """
    hash_value: int = 0
    for char in s:
        # Use the same algorithm as Rust: hash = ((hash * 31) + ord(char)) & mask
        # The mask 0x7FFFFFFFFFFFFFFF ensures we stay within 63 bits (positive i64 range)
        hash_value = ((hash_value * 31) + ord(char)) & 0x7FFFFFFFFFFFFFFF
    return abs(hash_value)


# Example usage:
if __name__ == "__main__":
    # Test with some binary names
    test_names = [
        "fibonacci_optimized.cwasm",
        "fibonacci.cwasm",
        "matrix_multiplication_component.cwasm",
        "image_classification_resnet_onnx.cwasm"
    ]
    
    print("Hash values for test binary names:")
    for name in test_names:
        hash_val = hash_binary_name(name)
        print(f"  {name}: {hash_val}")

