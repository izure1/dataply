export function bytesToNumber(bytes: Uint8Array, offset: number = 0, length: number = bytes.length): number {
  if (length === 4) {
    return (
      (bytes[offset] |
        (bytes[offset + 1] << 8) |
        (bytes[offset + 2] << 16) |
        (bytes[offset + 3] << 24)) >>>
      0
    )
  }

  if (length === 8) {
    const low =
      (bytes[offset] |
        (bytes[offset + 1] << 8) |
        (bytes[offset + 2] << 16) |
        (bytes[offset + 3] << 24)) >>>
      0

    const high =
      (bytes[offset + 4] |
        (bytes[offset + 5] << 8) |
        (bytes[offset + 6] << 16) |
        (bytes[offset + 7] << 24)) >>>
      0

    return low + high * 4294967296
  }

  let low = 0
  const lenLow = length < 4 ? length : 4
  for (let i = 0; i < lenLow; i++) {
    low |= bytes[offset + i] << (i * 8)
  }
  low >>>= 0

  if (length > 4) {
    let high = 0
    const lenHigh = length < 8 ? length : 8
    for (let i = 4; i < lenHigh; i++) {
      high |= bytes[offset + i] << ((i - 4) * 8)
    }
    high >>>= 0
    return low + high * 4294967296
  }

  return low
}
