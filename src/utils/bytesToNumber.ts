export function bytesToNumber(bytes: Uint8Array, offset: number = 0, length: number = bytes.length): number {
  let low = 0
  const lenLow = Math.min(length, 4)
  for (let i = 0; i < lenLow; i++) {
    low |= bytes[offset + i] << (i * 8)
  }
  low >>>= 0

  if (length > 4) {
    let high = 0
    const lenHigh = Math.min(length, 8)
    for (let i = 4; i < lenHigh; i++) {
      high |= bytes[offset + i] << ((i - 4) * 8)
    }
    high >>>= 0
    return low + high * 4294967296
  }

  return low
}
