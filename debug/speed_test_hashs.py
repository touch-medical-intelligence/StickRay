import hashlib
import timeit


def md5_hash(key):
    return int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)


def sha1_hash(key):
    return int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)


def sha256_hash(key):
    return int(hashlib.sha256(key.encode('utf-8')).hexdigest(), 16)


def fnv1a_hash(key, prime=0x01000193, offset_basis=0x811C9DC5):
    """Simple implementation of the FNV-1a hash algorithm."""
    hash_value = offset_basis
    for char in key:
        hash_value ^= ord(char)
        hash_value *= prime
        hash_value &= 0xFFFFFFFFFFFFFFFF  # to ensure 64-bit length
    return hash_value


def benchmark_hash_func(func, key="some_random_key"):
    setup_code = f"from __main__ import {func.__name__}"
    stmt = f"{func.__name__}('{key}')"
    times = timeit.repeat(stmt, setup_code, number=1000000, repeat=3)
    avg_time = sum(times) / len(times)
    print(f"Average time for {func.__name__}: {avg_time:.5f} seconds")


import random
import string
from scipy.stats import chisquare


def generate_random_string(length=10):
    """Generate a random string of a given length."""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def evalt_uniformity(hash_func, num_keys=100000, num_buckets=1000):
    """Test the uniformity of a hash function using the Chi-squared test."""
    observed_counts = [0] * num_buckets

    # Hash keys and distribute them into buckets
    for _ in range(num_keys):
        random_key = generate_random_string()
        bucket_index = hash_func(random_key) % num_buckets
        observed_counts[bucket_index] += 1

    # For a perfect uniform distribution, every bucket would have num_keys / num_buckets keys.
    expected_counts = [num_keys / num_buckets] * num_buckets
    chi2, p_val = chisquare(observed_counts, expected_counts)

    print(f"{hash_func.__name__}: Chi2 Value = {chi2}, P-value = {p_val:.4f}")


if __name__ == '__main__':

    hash_functions = [md5_hash, sha1_hash, sha256_hash, fnv1a_hash]

    for func in hash_functions:
        benchmark_hash_func(func)

    for func in hash_functions:
        evalt_uniformity(func)
