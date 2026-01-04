
import { KeyManager } from '../src/core/KeyManager'

describe('KeyManager', () => {
  const manager = new KeyManager()
  const SIZE_RID = 6

  test('should correctly set and get pageId', () => {
    const buffer = new Uint8Array(SIZE_RID)
    const pageId = 1000

    manager.setPageId(buffer, pageId)
    const result = manager.getPageId(buffer)

    expect(result).toBe(pageId)
  })

  test('should correctly set and get slotIndex', () => {
    const buffer = new Uint8Array(SIZE_RID)
    const slotIndex = 50

    manager.setSlotIndex(buffer, slotIndex)
    const result = manager.getSlotIndex(buffer)

    expect(result).toBe(slotIndex)
  })

  test('should correctly convert to and from numeric key', () => {
    const buffer = new Uint8Array(SIZE_RID)
    // 50 (slot) + 1000 (page) * 65536 = 65536050
    const expectedKey = 65536050

    manager.setBufferFromKey(expectedKey, buffer)
    const result = manager.toNumericKey(buffer)

    expect(result).toBe(expectedKey)
  })

  test('should maintain consistency between individual setters and numeric key', () => {
    const buffer = new Uint8Array(SIZE_RID)
    const pageId = 1000
    const slotIndex = 50

    manager.setPageId(buffer, pageId)
    manager.setSlotIndex(buffer, slotIndex)

    // PageID (1000 << 16) | SlotIndex (50)
    const expectedKey = (pageId * 65536) + slotIndex
    const numericKey = manager.toNumericKey(buffer)

    expect(numericKey).toBe(expectedKey)
  })
})
