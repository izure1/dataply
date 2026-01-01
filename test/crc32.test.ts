
import { crc32 } from '../src/utils/crc32';

describe('CRC32', () => {
  it('should calculate CRC32 for "123456789"', () => {
    const buffer = Buffer.from('123456789');
    expect(crc32(buffer)).toBe(0xCBF43926);
  });

  it('should calculate CRC32 for empty buffer', () => {
    const buffer = Buffer.from('');
    expect(crc32(buffer)).toBe(0x00000000);
  });

  it('should calculate CRC32 for "The quick brown fox jumps over the lazy dog"', () => {
    const buffer = Buffer.from('The quick brown fox jumps over the lazy dog');
    expect(crc32(buffer)).toBe(0x414FA339);
  });

  it('should handle Uint8Array correctly', () => {
    const buffer = new Uint8Array([0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39]); // "123456789"
    expect(crc32(buffer)).toBe(0xCBF43926);
  });
});
