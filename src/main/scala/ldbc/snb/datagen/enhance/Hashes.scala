package ldbc.snb.datagen.enhance

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

/**
  * Deterministic hash-based pseudo randomness.
  *
  * All enhancement modules derive their randomness from `xxhash64` over
  * entity ids plus a salt, so results are fully reproducible across runs
  * and require no joins or external state.
  */
object Hashes {

  /** Long mask: maps a hash to a non-negative value. */
  private val Mask63 = 0x7fffffffffffffffL

  /** Uniform double in [0, 1) derived from the given columns. */
  def hash01(cols: Column*): Column =
    (xxhash64(cols: _*).bitwiseAND(lit(Mask63)) / lit(Math.pow(2.0, 63)))

  /** Uniform long in [0, m). */
  def hashMod(cols: Column*)(m: Long): Column =
    pmod(xxhash64(cols: _*), lit(m))

  /** True with probability p (p in [0,1]). */
  def chance(cols: Column*)(p: Double): Column = hash01(cols: _*) < lit(p)

  /** Uniform long in [lo, hi). */
  def hashRange(cols: Column*)(lo: Long, hi: Long): Column =
    hashMod(cols: _*)(hi - lo) + lit(lo)
}
