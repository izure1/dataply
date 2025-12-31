import { bytesToNumber, numberToBytes, setBit, getBit } from '../utils'

/**
 * 행은 헤더와 바디로 구성됩니다. 헤더는 3바이트, 바디는 최대 65535바이트입니다.
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
   * 삭제 여부를 반환합니다.
   * @param row 행 데이터
   * @returns 삭제 여부
   */
  getDeletedFlag(row: Uint8Array): boolean {
    return getBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_DELETED)
  }

  /**
   * 오버플로우 여부를 반환합니다.
   * @param row 행 데이터
   * @returns 오버플로우 여부
   */
  getOverflowFlag(row: Uint8Array): boolean {
    return getBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_OVERFLOW)
  }

  /**
   * 행의 크기를 반환합니다. 이는 순수하게 행의 데이터 크기만을 의미합니다.
   * @param row 행 데이터
   * @returns 행의 크기
   */
  getBodySize(row: Uint8Array): number {
    return bytesToNumber(row, Row.CONSTANT.OFFSET_BODY_SIZE, Row.CONSTANT.SIZE_BODY)
  }

  /**
   * 행의 고유값(PK)를 반환합니다.
   * @param row 행 데이터
   * @returns 행의 고유값(PK)
   */
  getPK(row: Uint8Array): number {
    return bytesToNumber(row, Row.CONSTANT.OFFSET_PK, Row.CONSTANT.SIZE_PK)
  }

  /**
   * 행의 바디를 반환합니다.
   * @param row 행 데이터
   * @returns 행의 바디
   */
  getBody(row: Uint8Array): Uint8Array {
    return row.subarray(Row.CONSTANT.SIZE_HEADER)
  }

  /**
   * 삭제 여부를 설정합니다.
   * @param row 행 데이터
   * @param deleted 삭제 여부
   */
  setDeletedFlag(row: Uint8Array, deleted: boolean): void {
    row[Row.CONSTANT.OFFSET_FLAG] = setBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_DELETED, deleted)
  }

  /**
   * 오버플로우 여부를 설정합니다.
   * @param row 행 데이터
   * @param overflow 오버플로우 여부
   */
  setOverflowFlag(row: Uint8Array, overflow: boolean): void {
    row[Row.CONSTANT.OFFSET_FLAG] = setBit(row[Row.CONSTANT.OFFSET_FLAG], Row.CONSTANT.FLAG_OVERFLOW, overflow)
  }

  /**
   * 행의 크기를 설정합니다.
   * @param row 행 데이터
   * @param rowSize 행의 크기
   */
  setBodySize(row: Uint8Array, rowSize: number): void {
    numberToBytes(rowSize, row, Row.CONSTANT.OFFSET_BODY_SIZE, Row.CONSTANT.SIZE_BODY)
  }

  /**
   * 행의 고유값(PK)를 설정합니다.
   * @param row 행 데이터
   * @param pk 행의 고유값(PK)
   */
  setPK(row: Uint8Array, pk: number): void {
    numberToBytes(pk, row, Row.CONSTANT.OFFSET_PK, Row.CONSTANT.SIZE_PK)
  }

  /**
   * 행의 바디를 설정합니다.
   * @param row 행 데이터
   * @param body 행의 바디
   */
  setBody(row: Uint8Array, body: Uint8Array): void {
    row.set(body, Row.CONSTANT.SIZE_HEADER)
  }
}
