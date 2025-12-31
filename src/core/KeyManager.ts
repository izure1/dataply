import { bytesToNumber, numberToBytes } from '../utils'
import { Row } from './Row'

/**
 * Key Manager class.
 */
export class KeyManager {
  /**
   * Returns a numeric key from the buffer.
   * @param buffer Buffer
   * @returns Numeric key
   */
  toNumericKey(buffer: Uint8Array): number {
    return bytesToNumber(buffer)
  }

  /**
   * Sets a numeric key in the buffer.
   * @param key Numeric key
   * @param buffer Buffer
   * @returns Buffer
   */
  setBufferFromKey(key: number, buffer: Uint8Array): Uint8Array {
    return numberToBytes(key, buffer)
  }

  /**
   * Returns the Page ID from the buffer.
   * @param buffer Buffer
   * @returns Page ID
   */
  getPageId(buffer: Uint8Array): number {
    return bytesToNumber(buffer, 2, 4)
  }

  /**
   * Sets the Page ID in the buffer.
   * @param buffer Buffer
   * @param pageId Page ID
   */
  setPageId(buffer: Uint8Array, pageId: number): void {
    numberToBytes(pageId, buffer, 2, 4)
  }

  /**
   * Returns the Slot Index from the buffer.
   * @param buffer Buffer
   * @returns Slot index
   */
  getSlotIndex(buffer: Uint8Array): number {
    return bytesToNumber(buffer, 0, 2)
  }

  /**
   * Sets the Slot Index in the buffer.
   * @param buffer Buffer
   * @param slotIndex Slot index
   */
  setSlotIndex(buffer: Uint8Array, slotIndex: number): void {
    numberToBytes(slotIndex, buffer, 0, 2)
  }
}
