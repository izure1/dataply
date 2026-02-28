type SupportedNumberArray =
  | number[]
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Float16Array
  | Float32Array
  | Float64Array

export function getMinValue(array: SupportedNumberArray): number {
  let i = 0
  let min = Infinity
  let len = array.length
  while (i < len) {
    if (array[i] < min) {
      min = array[i]
    }
    i++
  }
  return min
}

export function getMaxValue(array: SupportedNumberArray): number {
  let i = 0
  let max = -Infinity
  let len = array.length
  while (i < len) {
    if (array[i] > max) {
      max = array[i]
    }
    i++
  }
  return max
}

export function getMinMaxValue(array: SupportedNumberArray): [number, number] {
  let i = 0
  let min = Infinity
  let max = -Infinity
  let len = array.length
  while (i < len) {
    if (array[i] < min) {
      min = array[i]
    }
    if (array[i] > max) {
      max = array[i]
    }
    i++
  }
  return [min, max]
}
