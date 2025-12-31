export function numberToBytes(value: number, buffer: Uint8Array, offset: number = 0, length: number = buffer.length): Uint8Array {
  // 32비트 이하일 경우 빠른 처리
  if (value <= 0xffffffff && length <= 4) {
    for (let i = 0; i < length; i++) {
      buffer[offset + i] = value & 0xff
      value >>>= 8
    }
    return buffer
  }

  // 32비트 초과 또는 길이가 긴 경우 Low/High 분리
  let low = value >>> 0
  let high = Math.floor(value / 4294967296)

  const lenLow = Math.min(length, 4)
  for (let i = 0; i < lenLow; i++) {
    buffer[offset + i] = low & 0xff
    low >>>= 8
  }

  if (length > 4) {
    const lenHigh = Math.min(length, 8)
    for (let i = 4; i < lenHigh; i++) {
      buffer[offset + i] = high & 0xff
      high >>>= 8
    }
  }

  return buffer
}
