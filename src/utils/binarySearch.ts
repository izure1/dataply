/**
 * Performs a binary search on a sorted array.
 * @param array Sorted array to search in
 * @param comparator Function that returns 0 if the element matches, < 0 if the target is before, > 0 if the target is after
 * @returns Index of the element if found, -1 otherwise
 */
export function binarySearch<T>(
  array: ArrayLike<T>,
  comparator: (element: T) => number
): number {
  let low = 0
  let high = array.length - 1

  while (low <= high) {
    const mid = (low + high) >>> 1
    const compare = comparator(array[mid])

    if (compare === 0) {
      return mid
    } else if (compare < 0) {
      low = mid + 1
    } else {
      high = mid - 1
    }
  }

  return -1
}
/**
 * Performs a binary search on a sorted numeric array.
 * @param array Sorted numeric array to search in
 * @param target Numeric value to search for
 * @returns Index of the element if found, -1 otherwise
 */
export function binarySearchNumeric(
  array: ArrayLike<number>,
  target: number
): number {
  let low = 0
  let high = array.length - 1

  while (low <= high) {
    const mid = (low + high) >>> 1
    const val = array[mid]

    if (val === target) {
      return mid
    } else if (val < target) {
      low = mid + 1
    } else {
      high = mid - 1
    }
  }

  return -1
}
