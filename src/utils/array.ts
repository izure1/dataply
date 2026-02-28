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
  while (i < array.length) {
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
  while (i < array.length) {
    if (array[i] > max) {
      max = array[i]
    }
    i++
  }
  return max
}
