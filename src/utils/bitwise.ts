/**
 * Sets or clears a specific bit in a 1-byte number and returns it.
 * (Uses index 0-7 from the right, LSB-first)
 * @param value Target number (1 byte)
 * @param bitPos Bit position (0-7, 0 is the rightmost/least significant bit)
 * @param flag Value to set (true: 1, false: 0)
 * @returns Modified number
 */
export function setBit(value: number, bitPos: number, flag: boolean): number {
  if (flag) {
    return (value | (1 << bitPos)) >>> 0
  } else {
    return (value & ~(1 << bitPos)) >>> 0
  }
}

/**
 * Returns the value of a specific bit in a 1-byte number.
 * (Uses index 0-7 from the right, LSB-first)
 * @param value Target number (1 byte)
 * @param bitPos Bit position (0-7, 0 is the rightmost/least significant bit)
 * @returns Bit value (true: 1, false: 0)
 */
export function getBit(value: number, bitPos: number): boolean {
  return (value & (1 << bitPos)) !== 0
}
