fn main() {
    println!("node-0: {}", xxhash_rust::xxh3::xxh3_64(b"node-0"));
    println!("node-1: {}", xxhash_rust::xxh3::xxh3_64(b"node-1"));
    println!("node-2: {}", xxhash_rust::xxh3::xxh3_64(b"node-2"));
}
