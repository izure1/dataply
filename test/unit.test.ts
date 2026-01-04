import { numberToBytes, bytesToNumber, setBit, getBit } from '../src/utils'

describe('utils', () => {
  test('should set bit correctly (LSB-first)', () => {
    let value = 0
    value = setBit(value, 0, true) // 0000 0001
    expect(value).toBe(1)
    value = setBit(value, 7, true) // 1000 0001
    expect(value).toBe(129)
    value = setBit(value, 0, false) // 1000 0000
    expect(value).toBe(128)
  })

  test('should get bit correctly (LSB-first)', () => {
    const value = 5 // 0000 0101
    expect(getBit(value, 0)).toBe(true)  // 1
    expect(getBit(value, 1)).toBe(false) // 0
    expect(getBit(value, 2)).toBe(true)  // 1
    expect(getBit(value, 3)).toBe(false) // 0
    expect(getBit(value, 7)).toBe(false) // 0
  })
})

describe('numberToBytes', () => {
  test('should convert number to Uint8Array (4 bytes default)', () => {
    const result = numberToBytes(0x12345678, new Uint8Array(4))
    expect(result).toBeInstanceOf(Uint8Array)
    expect(result.length).toBe(4)
    // Little-Endian: 78 56 34 12
    expect(result[0]).toBe(0x78)
    expect(result[1]).toBe(0x56)
    expect(result[2]).toBe(0x34)
    expect(result[3]).toBe(0x12)
  })

  test('should handle varying byte lengths', () => {
    const result = numberToBytes(0x1234, new Uint8Array(2))
    expect(result.length).toBe(2)
    // Little-Endian: 34 12
    expect(result[0]).toBe(0x34)
    expect(result[1]).toBe(0x12)
  })

  test('should support offset and length', () => {
    const buffer = new Uint8Array(10)
    numberToBytes(0x12345678, buffer, 2, 4)
    expect(buffer[2]).toBe(0x78)
    expect(buffer[3]).toBe(0x56)
    expect(buffer[4]).toBe(0x34)
    expect(buffer[5]).toBe(0x12)
    expect(buffer[0]).toBe(0)
    expect(buffer[6]).toBe(0)
  })
})

describe('bytesToNumber', () => {
  test('should convert Uint8Array to number', () => {
    // Little-Endian: 78 56 34 12 -> 0x12345678
    const bytes = new Uint8Array([0x78, 0x56, 0x34, 0x12])
    const result = bytesToNumber(bytes)
    expect(result).toBe(0x12345678)
  })

  test('should handle different lengths', () => {
    // Little-Endian: 34 12 -> 0x1234
    const bytes = new Uint8Array([0x34, 0x12])
    const result = bytesToNumber(bytes)
    expect(result).toBe(0x1234)
  })

  test('should support offset and length', () => {
    const bytes = new Uint8Array([0, 0, 0x78, 0x56, 0x34, 0x12, 0, 0])
    const result = bytesToNumber(bytes, 2, 4)
    expect(result).toBe(0x12345678)
  })

  test('should be reversible', () => {
    const val = 123456789
    const bytes = numberToBytes(val, new Uint8Array(4))
    const result = bytesToNumber(bytes)
    expect(result).toBe(val)
  })

  test('should handle large numbers correctly (within safe integer, 8 bytes)', () => {
    const val = 8589934591 // greater than 2^32
    const bytes = numberToBytes(val, new Uint8Array(8))
    const result = bytesToNumber(bytes)
    expect(result).toBe(val)
  })
})


