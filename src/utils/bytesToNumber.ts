// Pre-allocated shared buffer and DataView to avoid per-call allocation
const tempBuffer = new ArrayBuffer(8)
const tempView = new DataView(tempBuffer)
const tempArray = new Uint8Array(tempBuffer)

export function bytesToNumber(bytes: Uint8Array, offset: number = 0, length: number = bytes.length): number {
  // Fast copy to shared buffer (avoids DataView creation per call)
  tempArray.set(bytes.subarray(offset, offset + length))

  switch (length) {
    case 1:
      return tempView.getUint8(0)
    case 2:
      return tempView.getUint16(0, true)
    case 3:
      return tempView.getUint16(0, true) + (tempView.getUint8(2) << 16)
    case 4:
      return tempView.getUint32(0, true)
    case 5:
      return tempView.getUint32(0, true) + tempView.getUint8(4) * 4294967296
    case 6:
      return tempView.getUint32(0, true) + tempView.getUint16(4, true) * 4294967296
    case 7:
      return tempView.getUint32(0, true) + (tempView.getUint16(4, true) + (tempView.getUint8(6) << 16)) * 4294967296
    case 8:
      return tempView.getUint32(0, true) + tempView.getUint32(4, true) * 4294967296
    default:
      return 0
  }
}
