export function numberToBytes(value: number, buffer: Uint8Array, offset: number = 0, length: number = buffer.length): Uint8Array {
  // 4바이트 최적화
  if (length === 4) {
    buffer[offset] = value
    buffer[offset + 1] = value >>> 8
    buffer[offset + 2] = value >>> 16
    buffer[offset + 3] = value >>> 24
    return buffer
  }

  // 8바이트 최적화 (64비트 정수 처리)
  if (length === 8) {
    const low = value >>> 0
    const high = Math.floor(value / 4294967296)

    buffer[offset] = low
    buffer[offset + 1] = low >>> 8
    buffer[offset + 2] = low >>> 16
    buffer[offset + 3] = low >>> 24

    buffer[offset + 4] = high
    buffer[offset + 5] = high >>> 8
    buffer[offset + 6] = high >>> 16
    buffer[offset + 7] = high >>> 24
    return buffer
  }

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

  const lenLow = length < 4 ? length : 4
  for (let i = 0; i < lenLow; i++) {
    buffer[offset + i] = low & 0xff
    low >>>= 8
  }

  if (length > 4) {
    const lenHigh = length < 8 ? length : 8
    for (let i = 4; i < lenHigh; i++) {
      buffer[offset + i] = high & 0xff
      high >>>= 8
    }
  }

  return buffer
}
