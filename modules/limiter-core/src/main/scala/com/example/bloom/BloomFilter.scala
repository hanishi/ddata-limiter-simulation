package com.example.bloom

/** Probabilistic data structure for efficient membership testing.
  *
  * @param mBits The total number of bits in the Bloom filter's bit array.
  *              Determines the filter's capacity and false positive rate.
  * @param k The number of hash functions used.
  *          Controls how many bits are set or checked for each element.
  * @param words The underlying array of 64-bit words that stores the Bloom filter's bitset.
  */
final class BloomFilter private (val mBits: Int, val k: Int, private val words: Array[Long]) {

  /** Add an element to the Bloom filter. */
  def add(x: Long): Unit = {
    val (h1, h2) = hashes(x)
    var i        = 0
    while (i < k) {
      val bit = bitIndex(h1, h2, i)
      words(wordIx(bit)) |= bitMask(bit)
      i += 1
    }
  }

  @inline private def hashes(x: Long): (Long, Long) = {
    // Non-allocating, fast 64-bit mixers (SplitMix64-style) for double hashing
    val z1 = mix64(x ^ 0x9e3779b97f4a7c15L)
    val z2 = mix64(x ^ 0x94d049bb133111ebL)
    (z1 & 0x7fffffffffffffffL, z2 & 0x7fffffffffffffffL)
  }

  @inline private def mix64(z0: Long): Long = {
    var z = z0
    z ^= (z >>> 33)
    z *= 0xff51afd7ed558ccdL
    z ^= (z >>> 33)
    z *= 0xc4ceb9fe1a85ec53L
    z ^= (z >>> 33)
    z
  }

  @inline private def wordIx(bit: Int): Int = bit >>> 6

  @inline private def bitMask(bit: Int): Long = 1L << (bit & 63)

  @inline private def bitIndex(h1: Long, h2: Long, i: Int): Int = {
    val mix         = h1 + (i.toLong * h2)
    val nonNegative = mix & 0x7fffffffffffffffL
    (nonNegative % mBits).toInt
  }

  /** Test if an element might be in the set (may have false positives). */
  def maybeContains(x: Long): Boolean = {
    val (h1, h2) = hashes(x)
    var i        = 0
    var ok       = true
    while (i < k && ok) {
      val bit = bitIndex(h1, h2, i)
      val wIx = wordIx(bit)
      if (wIx < 0 || wIx >= words.length) return false // bounds check
      ok = (words(wIx) & bitMask(bit)) != 0L
      i += 1
    }
    ok
  }

  /** Create an immutable snapshot of the internal bit array. */
  def snapshotWords: Array[Long] = java.util.Arrays.copyOf(words, words.length)

  /** Load state from a snapshot, zeroing any tail elements. */
  def loadFromArray(a: Array[Long]): Unit = {
    val n = math.min(words.length, a.length)
    var i = 0
    while (i < n) { words(i) = a(i); i += 1 }
    while (i < words.length) { words(i) = 0L; i += 1 }
  }

  /** Reset all bits to 0. */
  def clear(): Unit = {
    var i = 0
    while (i < words.length) { words(i) = 0L; i += 1 }
  }

  /** Estimate the number of inserted elements based on bit density. */
  def estimateInsertCount: Int = {
    val m    = mBits.toDouble
    val X    = countSetBits.toDouble
    val one  = 1.0 - math.min(1.0, math.max(0.0, X / m))
    val frac = math.max(1e-12, one)
    math.max(0, math.round(-(m / k) * math.log(frac)).toInt)
  }

  /** Count the number of set bits in the filter. */
  def countSetBits: Long = {
    var i   = 0
    var acc = 0L
    val n   = words.length
    while (i < n) { acc += java.lang.Long.bitCount(words(i)); i += 1 }
    acc
  }
}

object BloomFilter {

  /** Create a new Bloom filter optimized for expected number of elements and false positive rate. */
  def create(expectedN: Int, fpr: Double): BloomFilter = {
    val (mBits, k) = sizing(expectedN, fpr)
    val words      = new Array[Long]((mBits + 63) >>> 6)
    new BloomFilter(mBits, k, words)
  }

  /** Calculate memory usage in bytes for given parameters. */
  def memoryUsage(expectedN: Int, fpr: Double): Long = {
    val (mBits, _) = sizing(expectedN, fpr)
    mBits / 8L // bits to bytes
  }

  /** Compute optimal m (bits) and k (hash functions) from expected n and target false-positive rate. */
  def sizing(expectedN: Int, fpr: Double): (Int, Int) = {
    val n        = math.max(1, expectedN)
    val f        = math.min(0.5, math.max(1e-9, fpr))
    val m        = math.ceil(-(n * math.log(f)) / (math.log(2) * math.log(2))).toInt
    val mAligned = ((m + 63) / 64) * 64 // multiple of 64 for word alignment
    val k        = math.max(1, math.round((mAligned.toDouble / n) * math.log(2)).toInt)
    (mAligned, k)
  }
}
