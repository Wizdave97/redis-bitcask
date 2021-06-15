use std::io::prelude::*;

const BYTES_PER_LINE: usize = 16;
const INPUT: &'static [u8] = br#"
fn main() {
    println!("Hello world");
}
"#;
fn main() {
    let mut buffer: Vec<u8> = vec![];
    INPUT.read_to_end(&mut buffer).unwrap();

    let mut position_in_file = 0;

    for line in buffer.chunks(BYTES_PER_LINE) {
        println!("[0x{0:08x}]", position_in_file);
        for byte in line {
            println!("0x{0:02x}", byte);
        }
        println!();
        position_in_file += BYTES_PER_LINE;
    }
}
