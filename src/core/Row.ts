import { bytesToNumber, numberToBytes, setBit, getBit } from '../utils'

/**
 * A row consists of a header and a body. The header is 3 bytes, and the body can be up to 65535 bytes.
 */
export class Row {
  static readonly CONSTANT = {
    FLAG_DELETED: 0,
    FLAG_OVERFLOW: 2,
    SIZE_FLAG: 1,
    SIZE_BODY: 2,
    SIZE_PK: 6,
    SIZE_RID: 6,
    SIZE_HEADER: 9,
    OFFSET_FLAG: 0,
    OFFSET_BODY_SIZE: 1,
    OFFSET_PK: 3,
  } as const

  /**
   * Returns whether the row is deleted.
   * @param row Row data
   * @returns Whether deleted
   */
  getDeletedFlag(row: Uint8Array): boolean {
    return getBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_DELETED)
  }

  /**
   * Returns whether the row is overflowed.
   * @param row Row data
   * @returns Whether overflowed
   */
  getOverflowFlag(row: Uint8Array): boolean {
    return getBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_OVERFLOW)
  }

  /**
   * Returns the size of the row body. This represents purely the data size of the row.
   * @param row Row data
   * @returns Body size
   */
  getBodySize(row: Uint8Array): number {
    return bytesToNumber(row, Row.CONSTANT.OFFSET_BODY_SIZE, Row.CONSTANT.SIZE_BODY)
  }

  /**
   * Returns the primary key (PK) of the row.
   * @param row Row data
   * @returns Primary key (PK)
   */
  getPK(row: Uint8Array): number {
    return bytesToNumber(row, Row.CONSTANT.OFFSET_PK, Row.CONSTANT.SIZE_PK)
  }

  /**
   * Returns the row body.
   * @param row Row data
   * @returns Row body
   */
  getBody(row: Uint8Array): Uint8Array {
    return row.subarray(Row.CONSTANT.SIZE_HEADER)
  }

  /**
   * Sets whether the row is deleted.
   * @param row Row data
   * @param deleted Whether deleted
   */
  setDeletedFlag(row: Uint8Array, deleted: boolean): void {
    row[Row.CONSTANT.OFFSET_FLAG] = setBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_DELETED, deleted)
  }

  /**
   * Sets whether the row is overflowed.
   * @param row Row data
   * @param overflow Whether overflowed
   */
  setOverflowFlag(row: Uint8Array, overflow: boolean): void {
    row[Row.CONSTANT.OFFSET_FLAG] = setBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_OVERFLOW, overflow)
  }

  /**
   * Sets the size of the row body.
   * @param row Row data
   * @param rowSize Body size
   */
  setBodySize(row: Uint8Array, rowSize: number): void {
    numberToBytes(rowSize, row, Row.CONSTANT.OFFSET_BODY_SIZE, Row.CONSTANT.SIZE_BODY)
  }

  /**
   * Sets the primary key (PK) of the row.
   * @param row Row data
   * @param pk Primary key (PK)
   */
  setPK(row: Uint8Array, pk: number): void {
    numberToBytes(pk, row, Row.CONSTANT.OFFSET_PK, Row.CONSTANT.SIZE_PK)
  }

  /**
   * Sets the row body.
   * @param row Row data
   * @param body Row body
   */
  setBody(row: Uint8Array, body: Uint8Array): void {
    row.set(body, Row.CONSTANT.SIZE_HEADER)
  }
}
