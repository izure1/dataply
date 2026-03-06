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

function calcThreshold(sortedGaps: Float64Array, n: number): number {
  const gLen = sortedGaps.length
  if (gLen === 0) return 0

  const median = sortedGaps[Math.floor(gLen * 0.5)]
  const q1 = sortedGaps[Math.floor(gLen * 0.25)]
  const q3 = sortedGaps[Math.floor(gLen * 0.75)]
  const iqr = q3 - q1

  // 데이터 수에 따른 로그 스케일 가중치 (데이터가 많을수록 허용 오차를 넓혀 파편화 방지)
  const logN = Math.max(1, Math.log10(n))

  if (iqr > 0) {
    // 일반 케이스: 안정적인 IQR (사분위 범위) 기반 이상치 탐지
    // Q3를 베이스로 하여 롱테일 극단값에 대비하고 logN 가중치 적용
    const threshold = q3 + (iqr * 1.5 * logN)

    // 단, 최소한의 점프 폭 보장 (너무 미세한 파편화 방지)
    const minJump = Math.max(median * 5, 20)
    return Math.max(threshold, minJump)
  }

  // iqr === 0 인 경우: 절반 이상의 gap이 완벽히 동일 (예: PK 1씩 증가)
  const baseGap = median > 0 ? median : 1

  // 극단값을 제외한 90% 수준의 gap을 기준으로 변동(Jump)을 관찰
  const p90 = sortedGaps[Math.floor(gLen * 0.90)]

  if (p90 > baseGap) {
    // 상위 10%에 점프가 존재한다면 (예: 1, 1, ..., 995, 3997)
    // baseGap과 p90 사이의 중간 지점을 threshold로 설정
    const threshold = baseGap + (p90 - baseGap) * 0.5 * logN
    return Math.max(threshold, baseGap * 5, 20)
  }

  // 90% 이상이 균일하고 소수의 엄청난 극단값만 남았다면 (예: 1, 1, ..., 10000, 20000)
  // 전체 데이터의 평균 및 표준편차를 구하여 풀백
  let mean = 0
  for (let i = 0; i < gLen; i++) mean += sortedGaps[i]
  mean /= gLen

  let variance = 0
  for (let i = 0; i < gLen; i++) {
    const d = sortedGaps[i] - mean
    variance += d * d
  }
  const stddev = Math.sqrt(variance / gLen)

  if (stddev === 0) {
    // 완벽하게 균일한 분포 (예: 모든 PK가 1씩 증가)
    return baseGap * 2
  }

  // 표준편차를 활용한 이상치 임계값 설정
  const threshold = mean + (stddev * logN)
  return Math.max(threshold, baseGap * 5, 20)
}

/**
 * Sorts the input array and splits it into clusters based on the gaps between consecutive elements.
 * @param numbers Array of numbers to cluster
 * @param maxGap Optional fixed gap threshold. If not provided, it is calculated automatically.
 * @returns Array of clusters
 */
export function clusterNumbers(numbers: number[] | Float64Array, maxGap?: number): Float64Array[] {
  const n = numbers.length
  if (n === 0) return []
  if (n === 1) return [new Float64Array([numbers[0]])]

  // ── 1. Sort (copy first to avoid mutating input)
  const sorted = (
    numbers instanceof Float64Array ? numbers.slice() : Float64Array.from(numbers)
  ).sort()

  // ── 2. Compute gaps
  const gaps = new Float64Array(n - 1)
  for (let i = 0, len = n - 1; i < len; i++) {
    gaps[i] = sorted[i + 1] - sorted[i]
  }

  // ── 3. Determine threshold
  const sortedGaps = gaps.slice().sort()
  let threshold: number

  if (maxGap !== undefined) {
    threshold = maxGap
  }
  else {
    threshold = calcThreshold(sortedGaps, n)
  }

  // ── 4. Split into clusters
  const clusters: Float64Array[] = []
  let clusterStart = 0

  for (let i = 0, len = n - 1; i < len; i++) {
    if (gaps[i] > threshold) {
      clusters.push(sorted.subarray(clusterStart, i + 1))
      clusterStart = i + 1
    }
  }
  clusters.push(sorted.subarray(clusterStart))

  return clusters
}
