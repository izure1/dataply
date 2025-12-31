import { bytesToNumber, numberToBytes } from '../utils'
import { Row } from './Row'

/**
 * 키 관리자 클래스
 */
export class KeyManager {
  /**
   * 버퍼에서 숫자 키를 반환합니다.
   * @param buffer 버퍼
   * @returns 숫자 키
   */
  toNumericKey(buffer: Uint8Array): number {
    return bytesToNumber(buffer)
  }

  /**
   * 숫자 키를 버퍼에 설정합니다.
   * @param key 숫자 키
   * @param buffer 버퍼
   * @returns 버퍼
   */
  setBufferFromKey(key: number, buffer: Uint8Array): Uint8Array {
    return numberToBytes(key, buffer)
  }

  /**
   * 버퍼에서 페이지 ID를 반환합니다.
   * @param buffer 버퍼
   * @returns 페이지 ID
   */
  /**
   * 버퍼에서 페이지 ID를 반환합니다.
   * @param buffer 버퍼
   * @returns 페이지 ID
   */
  getPageId(buffer: Uint8Array): number {
    return bytesToNumber(buffer, 2, 4)
  }

  /**
   * 버퍼에 페이지 ID를 설정합니다.
   * @param buffer 버퍼
   * @param pageId 페이지 ID
   */
  setPageId(buffer: Uint8Array, pageId: number): void {
    numberToBytes(pageId, buffer, 2, 4)
  }

  /**
   * 버퍼에서 슬롯 인덱스를 반환합니다.
   * @param buffer 버퍼
   * @returns 슬롯 인덱스
   */
  getSlotIndex(buffer: Uint8Array): number {
    return bytesToNumber(buffer, 0, 2)
  }

  /**
   * 버퍼에 슬롯 인덱스를 설정합니다.
   * @param buffer 버퍼
   * @param slotIndex 슬롯 인덱스
   */
  setSlotIndex(buffer: Uint8Array, slotIndex: number): void {
    numberToBytes(slotIndex, buffer, 0, 2)
  }
}
