/**
 * Pre-computed CRC32 table for polynomial 0xEDB88320
 */
const CRC_TABLE = new Int32Array(256);

(function makeTable() {
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) {
      c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    }
    CRC_TABLE[i] = c;
  }
})();

/**
 * Calculates the CRC32 checksum of a buffer.
 * @param buf The buffer to calculate the checksum for.
 * @returns The CRC32 checksum as an unsigned integer.
 */
export function crc32(buf: Uint8Array | Buffer): number {
  let crc = -1; // 0xFFFFFFFF

  for (let i = 0; i < buf.length; i++) {
    crc = (crc >>> 8) ^ CRC_TABLE[(crc ^ buf[i]) & 0xFF];
  }

  return (crc ^ -1) >>> 0;
}
