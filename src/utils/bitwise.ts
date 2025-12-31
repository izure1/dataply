/**
 * 1바이트 숫자의 특정 비트를 설정하거나 해제하여 반환합니다.
 * (오른쪽부터 0 ~ 7 인덱스 사용, LSB-first)
 * @param value 대상 숫자 (1바이트)
 * @param bitPos 비트 위치 (0-7, 0이 가장 오른쪽/최하위 비트)
 * @param flag 설정할 값 (true: 1, false: 0)
 * @returns 변경된 숫자
 */
export function setBit(value: number, bitPos: number, flag: boolean): number {
  if (flag) {
    return (value | (1 << bitPos)) >>> 0
  } else {
    return (value & ~(1 << bitPos)) >>> 0
  }
}

/**
 * 1바이트 숫자의 특정 비트 값을 가져옵니다.
 * (오른쪽부터 0 ~ 7 인덱스 사용, LSB-first)
 * @param value 대상 숫자 (1바이트)
 * @param bitPos 비트 위치 (0-7, 0이 가장 오른쪽/최하위 비트)
 * @returns 비트 값 (true: 1, false: 0)
 */
export function getBit(value: number, bitPos: number): boolean {
  return (value & (1 << bitPos)) !== 0
}
