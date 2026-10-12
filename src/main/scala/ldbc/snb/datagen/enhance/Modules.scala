package ldbc.snb.datagen.enhance

import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/** F2: deterministic bijective person-id remap concentrating hot entities into one partition band. */
object IdRemap extends Logging {

  import ldbc.snb.datagen.generator.DatagenParams

  /** Person foreign-key columns per entity (entity display name -> columns to remap). */
  private val PersonFkCols: Map[String, Seq[String]] = Map(
    "Person"                         -> Seq("id"),
    "Person -[Knows]-> Person"       -> Seq("Person1Id", "Person2Id"),
    "Person -[HasInterest]-> Tag"    -> Seq("PersonId"),
    "Person -[StudyAt]-> University" -> Seq("PersonId"),
    "Person -[WorkAt]-> Company"     -> Seq("PersonId"),
    "Forum"                          -> Seq("ModeratorPersonId"),
    "Forum -[HasMember]-> Person"    -> Seq("PersonId"),
    "Post"                           -> Seq("CreatorPersonId"),
    "Comment"                        -> Seq("CreatorPersonId"),
    "Person -[Likes]-> Post"         -> Seq("PersonId"),
    "Person -[Likes]-> Comment"      -> Seq("PersonId")
  )

  private val Knows = "Person -[Knows]-> Person"

  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph =
    cfg.remapEnabled.fork.foldLeft(g)((g, _) => {
      // Same JVM run: GenerationStage has initialized DatagenParams before TransformationStage.
      val n       = DatagenParams.numPersons
      val numBits = math.ceil(math.log(math.ceil(n.toDouble / DatagenParams.blockSize)) / math.log(2)).toLong
      val k       = cfg.remapHotEvery
      val m       = (n + k - 1) / k
      log.info(s"enhance.IdRemap: bijective person id remap numPersons=$n numBits=$numBits K=$k hot band [0,$m)")

      val updates = g.entities.map { case (tpe, df) =>
        PersonFkCols.get(tpe.toString).fold(tpe -> df) { cols =>
          tpe -> remapCols(df, cols, k, m, numBits, sortPair = tpe.toString == Knows)
        }
      }
      replaceAll(g, updates.toSeq)
    })

  /**
    * Deterministic bijective remap of a person id column.
    *
    * Person ids are SN.formId(SN.composeId(seq, creationDate), blockId):
    *   id = ((bucket<<36 | seq) >> 20 << (20+numBits)) | (blockId << 20) | (low 20 bits)
    * with seq dense in [0, numPersons), bucket derived from creationDate and
    * numBits = ceil(log2(ceil(numPersons/blockSize))). The remap acts on seq:
    *   hot  (seq % K == 0): seq/K                -- dense into hot band [0, M), M = ceil(N/K)
    *   cold:                M + seq - ceil(seq/K) -- into [M, N)
    * Hot people additionally get bucket=0 and blockId=0, which collapses their id to
    * the bare seq (a contiguous band starting at 0); cold people keep their original
    * bucket/blockId so their id structure is preserved. Pure bit expressions:
    * no shuffle, no join, no window.
    */
  private def remapPersonId(idCol: Column, k: Long, m: Long, numBits: Long): Column = {
    val mask20    = lit(0xFFFFFL)
    val maskSeq   = lit((1L << 36) - 1)
    val maskBlock = lit((1L << numBits) - 1)
    val low20   = idCol.bitwiseAND(mask20)
    val blockId = shiftrightunsigned(idCol, 20).bitwiseAND(maskBlock)
    val mid     = shiftleft(shiftrightunsigned(idCol, 20 + numBits.toInt), 20).bitwiseOR(low20)
    val seq     = mid.bitwiseAND(maskSeq)
    val bucket  = shiftrightunsigned(mid, 36)
    val isHot   = seq % lit(k) === lit(0)
    val newSeq  = when(isHot, floor(seq / lit(k)))
      .otherwise(lit(m) + seq - floor((seq + lit(k - 1)) / lit(k)))
    val newBucket  = when(isHot, lit(0L)).otherwise(bucket)
    val newBlockId = when(isHot, lit(0L)).otherwise(blockId)
    val newMid = shiftleft(newBucket, 36).bitwiseOR(newSeq)
    shiftleft(shiftrightunsigned(newMid, 20), (20 + numBits).toInt)
      .bitwiseOR(shiftleft(newBlockId, 20))
      .bitwiseOR(newMid.bitwiseAND(mask20))
  }

  private def remapCols(df: DataFrame, cols: Seq[String], k: Long, m: Long, numBits: Long, sortPair: Boolean): DataFrame = {
    // In-place projection: replace each person-FK column by its remapped expression.
    // (Selecting the new column under the same name and dropping the old one would
    // drop both, as Spark columns are multi-set by name.)
    val remapSet = cols.toSet
    val exprs: Seq[Column] =
      if (sortPair) {
        // Knows is stored undirected with Person1Id <= Person2Id; the remap is not
        // monotonic, so re-normalise the pair to preserve that invariant downstream.
        val Seq(c1n, c2n) = cols
        val c1 = remapPersonId(col(c1n), k, m, numBits)
        val c2 = remapPersonId(col(c2n), k, m, numBits)
        df.columns.map {
          case `c1n` => least(c1, c2).as(c1n)
          case `c2n` => greatest(c1, c2).as(c2n)
          case other => col(other)
        }
      } else {
        df.columns.map {
          case c if remapSet(c) => remapPersonId(col(c), k, m, numBits).as(c)
          case other            => col(other)
        }
      }
    df.select(exprs: _*)
  }
}

/** F3: topology skew – supernodes, hot messages, small components, fragmentation, asymmetry, R-MAT. */
object TopologySkew extends Logging {
  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = g
}

/** F4: Zipf long-tail for attribute values and tag assignments. */
object ZipfSkew extends Logging {
  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = g
}

/** F5: temporal burst windows and hot/cold data split. */
object TemporalBurst extends Logging {
  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = g
}

/** F6: dirty data injection (nulls, self-loops, duplicates, dangling FKs, invalid values). */
object DirtyData extends Logging {
  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = g
}
