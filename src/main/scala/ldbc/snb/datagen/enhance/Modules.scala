package ldbc.snb.datagen.enhance

import ldbc.snb.datagen.syntax._
import ldbc.snb.datagen.util.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

/** Shared remap context derived from the generator's static parameters (initialized by GenerationStage). */
private[enhance] case class RemapCtx(n: Long, numBits: Long, k: Long, m: Long)

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

  /** Remap context (numPersons, formId bit layout, hot-every K, hot band size M). */
  private[enhance] def ctxOf(cfg: EnhanceConfig): RemapCtx = {
    val n       = DatagenParams.numPersons
    val numBits = math.ceil(math.log(math.ceil(n.toDouble / DatagenParams.blockSize)) / math.log(2)).toLong
    val k       = cfg.remapHotEvery
    RemapCtx(n, numBits, k, (n + k - 1) / k)
  }

  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph =
    cfg.remapEnabled.fork.foldLeft(g)((g, _) => {
      val ctx = ctxOf(cfg)
      log.info(s"enhance.IdRemap: bijective person id remap numPersons=${ctx.n} numBits=${ctx.numBits} " +
        s"K=${ctx.k} hot band [0,${ctx.m})")

      val updates = g.entities.map { case (tpe, df) =>
        PersonFkCols.get(tpe.toString).fold(tpe -> df) { cols =>
          tpe -> remapCols(df, cols, ctx, sortPair = tpe.toString == Knows)
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
  private[enhance] def remapPersonId(idCol: Column, ctx: RemapCtx): Column = {
    val k = ctx.k; val m = ctx.m; val numBits = ctx.numBits
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

  /** Extracts the dense seq segment (36 bits) from an SN.formId packed id (same layout for all entities). */
  private[enhance] def extractSeq(idCol: Column, ctx: RemapCtx): Column =
    shiftleft(shiftrightunsigned(idCol, 20 + ctx.numBits.toInt), 20)
      .bitwiseOR(idCol.bitwiseAND(lit(0xFFFFFL)))
      .bitwiseAND(lit((1L << 36) - 1))

  private def remapCols(df: DataFrame, cols: Seq[String], ctx: RemapCtx, sortPair: Boolean): DataFrame = {
    // In-place projection: replace each person-FK column by its remapped expression.
    // (Selecting the new column under the same name and dropping the old one would
    // drop both, as Spark columns are multi-set by name.)
    val remapSet = cols.toSet
    val exprs: Seq[Column] =
      if (sortPair) {
        // Knows is stored undirected with Person1Id <= Person2Id; the remap is not
        // monotonic, so re-normalise the pair to preserve that invariant downstream.
        val Seq(c1n, c2n) = cols
        val c1 = remapPersonId(col(c1n), ctx)
        val c2 = remapPersonId(col(c2n), ctx)
        df.columns.map {
          case `c1n` => least(c1, c2).as(c1n)
          case `c2n` => greatest(c1, c2).as(c2n)
          case other => col(other)
        }
      } else {
        df.columns.map {
          case c if remapSet(c) => remapPersonId(col(c), ctx).as(c)
          case other            => col(other)
        }
      }
    df.select(exprs: _*)
  }
}

/** F3: topology skew – supernodes, hot messages, small components, fragmentation, asymmetry, R-MAT. */
object TopologySkew extends Logging {

  import org.apache.spark.sql.functions.broadcast

  /** Runs `f` when `cond` is on, otherwise returns the graph unchanged. */
  private def whenOn(cond: Boolean)(f: RawGraph => RawGraph): RawGraph => RawGraph =
    g => cond.fork.foldLeft(g)((g, _) => f(g))

  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = {
    val needSupernodes = cfg.supernodeEnabled || cfg.hotmsgEnabled || cfg.supernodeLikeBurstEnabled
    val ctx            = IdRemap.ctxOf(cfg)
    // supernodes -> hot messages -> small components -> R-MAT -> fragmentation -> asymmetry
    val steps: Seq[RawGraph => RawGraph] = Seq(
      whenOn(needSupernodes)(withSupernodes(_, cfg, ctx)),
      whenOn(cfg.smallcompEnabled)(withSmallComponents(_, cfg, ctx)),
      whenOn(cfg.rmatEnabled)(withRmatSubgraph(_, cfg, ctx)),
      whenOn(cfg.fragmentDropKnowsRatio > 0)(withFragmentation(_, cfg)),
      whenOn(cfg.unidirFlipRatio > 0)(withAsymmetry(_, cfg))
    )
    val out = steps.foldLeft(g)((g, step) => step(g))
    if (out ne g) log.info("enhance.TopologySkew: applied topology skew modules")
    out
  }

  // ---- supernode detection -------------------------------------------------

  /**
    * Supernode set S: a hash sample of the hot band [0, M). All supernode-aware
    * features (extra knows edges, hot messages, like storms) hang off this set.
    * Returns a cached DataFrame(id, creationDate, deletionDate) suitable for broadcast.
    */
  private def supernodeSet(g: RawGraph, cfg: EnhanceConfig, ctx: RemapCtx) = {
    val (_, personDf) = entity(g, "Person")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Person entity missing"))
    personDf
      .where(col("id") < lit(ctx.m))
      .where(Hashes.hashMod(col("id"), lit(cfg.salt))(10000) < lit(cfg.supernodeRate * 10000))
      // renamed to `sid` so it never collides with the entity-side `id` column on joins
      .select(col("id").as("sid"), col("creationDate"), col("deletionDate"))
      .cache()
  }

  // ---- supernode extra edges ----------------------------------------------

  private def withSupernodes(g: RawGraph, cfg: EnhanceConfig, ctx: RemapCtx): RawGraph = {
    val s = supernodeSet(g, cfg, ctx)
    var out = g
    if (cfg.supernodeEnabled) out = addSupernodeKnows(out, cfg, ctx, s)
    if (cfg.hotmsgEnabled) out = addHotMessages(out, cfg, ctx, s)
    out
  }

  /**
    * Extra knows edges from supernodes. Targets are sampled from the hot band
    * [0, M): every id there is guaranteed to exist (it is the bare-seq band
    * produced by IdRemap), so no dangling FKs and no lookup joins are needed --
    * and the additional degree concentrates exactly in the hot partitions.
    */
  private def addSupernodeKnows(g: RawGraph, cfg: EnhanceConfig, ctx: RemapCtx, s: DataFrame): RawGraph = {
    val name  = "Person -[Knows]-> Person"
    val (_, knows) = entity(g, name)
      .getOrElse(throw new IllegalStateException(s"enhance.TopologySkew: $name entity missing"))
    val degree = greatest(
      lit(cfg.supernodeMinExtra),
      least(lit(cfg.supernodeMaxExtraDegreeMul),
        Hashes.hashMod(col("sid"), lit("deg"), lit(cfg.salt))(cfg.supernodeMaxExtraDegreeMul))
    )
    val extra = s
      .select(col("sid").as("p1"), col("creationDate").as("cd"), col("deletionDate").as("dd"),
        explode(sequence(lit(0L), degree - lit(1))).as("j"))
      .select(
        (col("cd") + Hashes.hashMod(col("p1"), col("j"), lit("cd"), lit(cfg.salt))(86400000L)).as("creationDate"),
        col("dd").as("deletionDate"),
        lit(false).as("explicitlyDeleted"),
        least(col("p1"), Hashes.hashMod(col("p1"), col("j"), lit("tgt"), lit(cfg.salt))(ctx.m)).as("Person1Id"),
        greatest(col("p1"), Hashes.hashMod(col("p1"), col("j"), lit("tgt"), lit(cfg.salt))(ctx.m)).as("Person2Id")
      )
      .where(col("Person1Id") =!= col("Person2Id"))
      .distinct()
    val merged = knows.unionByName(extra.select(knows.columns.map(col): _*))
    log.info(s"enhance.TopologySkew: added supernode knows edges")
    replace(g, entityType(g, name), merged)
  }

  // ---- hot messages --------------------------------------------------------

  private def addHotMessages(g: RawGraph, cfg: EnhanceConfig, ctx: RemapCtx, s: DataFrame): RawGraph = {
    var out = g
    val (_, posts) = entity(g, "Post")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Post entity missing"))
    // only the `sid` column is carried over from s, so no ambiguous references after the join
    val hotPosts = posts.join(broadcast(s.select(col("sid"))), col("CreatorPersonId") === col("sid"))
      .select(posts.columns.map(col): _*)
      .cache()
    val hotCount = hotPosts.count()
    if (hotCount == 0) {
      log.info("enhance.TopologySkew: hotmsg enabled but no hot posts found")
      return out
    }
    log.info(s"enhance.TopologySkew: $hotCount hot posts from supernodes")

    // likes on hot posts, likers sampled from the hot band (always-existing ids)
    val (_, likesPost) = entity(g, "Person -[Likes]-> Post")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Person -[Likes]-> Post entity missing"))
    val extraLikes = hotPosts
      .select(col("id").as("postId"), col("creationDate").as("pcd"), col("deletionDate").as("pdd"),
        explode(sequence(lit(0L), lit(cfg.hotmsgLikes - 1))).as("j"))
      .select(
        (col("pcd") + Hashes.hashMod(col("postId"), col("j"), lit("likecd"), lit(cfg.salt))(86400000L)).as("creationDate"),
        col("pdd").as("deletionDate"),
        lit(false).as("explicitlyDeleted"),
        Hashes.hashMod(col("postId"), col("j"), lit("liker"), lit(cfg.salt))(ctx.m).as("PersonId"),
        col("postId").as("PostId")
      )
      .distinct()
    out = replace(out, entityType(g, "Person -[Likes]-> Post"),
      likesPost.unionByName(extraLikes.select(likesPost.columns.map(col): _*)))

    // reply comments to hot posts, ids allocated in the reserved band (bucket 254)
    val (_, comments) = entity(g, "Comment")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Comment entity missing"))
    val replyBase = 254L << 36
    val replies = hotPosts
      .select(col("id").as("postId"), col("creationDate").as("pcd"), col("deletionDate").as("pdd"),
        col("LocationCountryId").as("country"),
        explode(sequence(lit(0L), lit(cfg.hotmsgReplies - 1))).as("j"))
      .select(
        (col("pcd") + Hashes.hashMod(col("postId"), col("j"), lit("rpcd"), lit(cfg.salt))(86400000L)).as("creationDate"),
        col("pdd").as("deletionDate"),
        lit(false).as("explicitlyDeleted"),
        (lit(replyBase) + shiftleft(IdRemap.extractSeq(col("postId"), ctx), 20).bitwiseOR(col("j"))).as("id"),
        lit("10.0.0.1").as("locationIP"),
        lit("Chrome").as("browserUsed"),
        concat(lit("hot reply "), col("j")).as("content"),
        lit(20).as("length"),
        Hashes.hashMod(col("postId"), col("j"), lit("replier"), lit(cfg.salt))(ctx.m).as("CreatorPersonId"),
        col("country").as("LocationCountryId"),
        col("postId").as("ParentPostId"),
        lit(null).cast("long").as("ParentCommentId")
      )
    out = replace(out, entityType(g, "Comment"),
      comments.unionByName(replies.select(comments.columns.map(col): _*)))

    // likes on the reply comments
    val (_, likesComment) = entity(g, "Person -[Likes]-> Comment")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Person -[Likes]-> Comment entity missing"))
    val extraCommentLikes = replies
      .select(col("id").as("commentId"), col("creationDate").as("ccd"), col("deletionDate").as("cdd"),
        explode(sequence(lit(0L), lit(cfg.hotmsgLikes - 1))).as("j"))
      .select(
        (col("ccd") + Hashes.hashMod(col("commentId"), col("j"), lit("clcd"), lit(cfg.salt))(86400000L)).as("creationDate"),
        col("cdd").as("deletionDate"),
        lit(false).as("explicitlyDeleted"),
        Hashes.hashMod(col("commentId"), col("j"), lit("cliker"), lit(cfg.salt))(ctx.m).as("PersonId"),
        col("commentId").as("CommentId")
      )
      .distinct()
    out = replace(out, entityType(g, "Person -[Likes]-> Comment"),
      likesComment.unionByName(extraCommentLikes.select(likesComment.columns.map(col): _*)))

    // hot tags on hot posts
    val (_, tags) = entity(g, "Tag")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Tag entity missing"))
    val (_, postHasTag) = entity(g, "Post -[HasTag]-> Tag")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Post -[HasTag]-> Tag entity missing"))
    val tagCount = tags.agg(max(col("id")).cast("long")).head().getLong(0) + 1
    val extraTags = hotPosts
      .select(col("id").as("postId"), col("creationDate").as("pcd"), col("deletionDate").as("pdd"),
        explode(sequence(lit(0L), lit(cfg.hotmsgTags.toLong - 1))).as("j"))
      .select(
        col("pcd").as("creationDate"),
        col("pdd").as("deletionDate"),
        col("postId").as("PostId"),
        Hashes.hashMod(col("postId"), col("j"), lit("tag"), lit(cfg.salt))(tagCount).as("TagId")
      )
      .distinct()
    out = replace(out, entityType(g, "Post -[HasTag]-> Tag"),
      postHasTag.unionByName(extraTags.select(postHasTag.columns.map(col): _*)))

    out
  }

  // ---- small components ----------------------------------------------------

  /**
    * Synthetic isolated small components: `count` schema-conformant Person rows in
    * the reserved band (bucket 255, seq >= numPersons), grouped into cliques of at
    * most cliqueMax members. Keeps WCC/BFS aware of fragmented topologies.
    */
  private def withSmallComponents(g: RawGraph, cfg: EnhanceConfig, ctx: RemapCtx): RawGraph = {
    val count     = cfg.smallcompCount
    val cliqueMax = math.max(1, cfg.smallcompCliqueMax)
    val reserveBase = if (cfg.smallcompReserveBase > 0) cfg.smallcompReserveBase else (255L << 36) + ctx.n
    val (_, personDf) = entity(g, "Person")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Person entity missing"))
    val spark = personDf.sparkSession
    val timeBounds = personDf.agg(max(col("creationDate")), max(col("deletionDate"))).head()
    val now        = timeBounds.getLong(0)
    val deletion   = timeBounds.getLong(1)

    val frag = spark.range(count).select(
      (lit(reserveBase) + col("id")).as("id"),
      (lit(now) - (pmod(col("id"), lit(365L)) * lit(86400000L))).as("creationDate"),
      lit(deletion).as("deletionDate"),
      lit(false).as("explicitlyDeleted"),
      concat(lit("Frag"), col("id")).as("firstName"),
      lit("Fragment").as("lastName"),
      lit("male").as("gender"),
      (lit(now) - lit(20L * 365 * 86400000)).as("birthday"),
      lit("10.0.0.1").as("locationIP"),
      lit("Chrome").as("browserUsed"),
      pmod(Hashes.hashMod(col("id"), lit(cfg.salt))(10000), lit(1500L)).as("LocationCityId"),
      lit("en").as("language"),
      concat(lit("frag"), col("id"), lit("@fragment.io")).as("email")
    )
    // NOTE: align to the existing Person column order rather than hard-coding it.
    val fragAligned = frag.select(personDf.columns.map(col): _*)
    var out = replace(g, entityType(g, "Person"), personDf.unionByName(fragAligned))

    // clique edges within each component: same comp (integer grouping), a.id < b.id
    val (_, knows) = entity(out, "Person -[Knows]-> Person")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: knows entity missing"))
    val a = fragAligned.alias("a")
    val b = fragAligned.alias("b")
    val clique = a.join(b,
      (col("a.id") - pmod(col("a.id"), lit(cliqueMax.toLong))) ===
        (col("b.id") - pmod(col("b.id"), lit(cliqueMax.toLong))) &&
        col("a.id") < col("b.id"))
      .select(
        col("a.creationDate").as("creationDate"),
        col("a.deletionDate").as("deletionDate"),
        lit(false).as("explicitlyDeleted"),
        col("a.id").as("Person1Id"),
        col("b.id").as("Person2Id")
      )
    out = replace(out, entityType(out, "Person -[Knows]-> Person"),
      knows.unionByName(clique.select(knows.columns.map(col): _*)))
    log.info(s"enhance.TopologySkew: injected $count fragment persons in clique components (reserve band $reserveBase)")
    out
  }

  // ---- R-MAT subgraph ------------------------------------------------------

  /**
    * Graph500-style R-MAT power-law subgraph over `rmatNodes` reserved-band persons
    * (band continues after the small components). Edge targets are generated by a
    * fixed-depth bit recursion (a/b/c/d quadrants) as pure column expressions.
    */
  private def withRmatSubgraph(g: RawGraph, cfg: EnhanceConfig, ctx: RemapCtx): RawGraph = {
    val nodes = cfg.rmatNodes
    val (_, personDf) = entity(g, "Person")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: Person entity missing"))
    val spark = personDf.sparkSession
    val timeBounds = personDf.agg(max(col("creationDate")), max(col("deletionDate"))).head()
    val now        = timeBounds.getLong(0)
    val deletion   = timeBounds.getLong(1)
    // band: after small components if enabled
    val smallOffset = if (cfg.smallcompEnabled) cfg.smallcompCount else 0L
    val base = (if (cfg.smallcompReserveBase > 0) cfg.smallcompReserveBase else (255L << 36) + ctx.n) + smallOffset

    val rmatPersons = spark.range(nodes).select(
      (lit(base) + col("id")).as("id"),
      (lit(now) - (pmod(col("id"), lit(365L)) * lit(86400000L))).as("creationDate"),
      lit(deletion).as("deletionDate"),
      lit(false).as("explicitlyDeleted"),
      concat(lit("Rmat"), col("id")).as("firstName"),
      lit("Powerlaw").as("lastName"),
      lit("female").as("gender"),
      (lit(now) - lit(25L * 365 * 86400000)).as("birthday"),
      lit("10.0.0.2").as("locationIP"),
      lit("Firefox").as("browserUsed"),
      pmod(Hashes.hashMod(col("id"), lit("rc"), lit(cfg.salt))(10000), lit(1500L)).as("LocationCityId"),
      lit("en").as("language"),
      concat(lit("rmat"), col("id"), lit("@powerlaw.io")).as("email")
    )
    val rmatAligned = rmatPersons.select(personDf.columns.map(col): _*)
    var out = replace(g, entityType(g, "Person"), personDf.unionByName(rmatAligned))

    val (_, knows) = entity(out, "Person -[Knows]-> Person")
      .getOrElse(throw new IllegalStateException("enhance.TopologySkew: knows entity missing"))
    val edgesPerNode = math.max(1.0, cfg.rmatEdgesPerNode).toLong
    val src = rmatAligned.select(col("id").as("src"),
      explode(sequence(lit(0L), lit(edgesPerNode - 1))).as("j"))

    // fixed-depth R-MAT quadrant recursion, all in column expressions
    val levels = math.ceil(math.log(math.max(2, nodes).toDouble) / math.log(2)).toInt
    val aW = (cfg.rmatA * 10000).toLong
    val abW = ((cfg.rmatA + cfg.rmatB) * 10000).toLong
    val abcW = ((cfg.rmatA + cfg.rmatB + cfg.rmatC) * 10000).toLong
    var cur: Column = lit(0L)
    for (l <- 0 until levels) {
      val r = Hashes.hashMod(col("src"), col("j"), lit(l), lit(cfg.salt))(10000)
      val q = when(r < lit(aW), lit(0L)).when(r < lit(abW), lit(1L))
        .when(r < lit(abcW), lit(2L)).otherwise(lit(3L))
      cur = shiftleft(cur, 1).bitwiseOR(q)
    }
    val rmatEdges = src.select(
      (lit(now) - (pmod(col("src"), lit(365L)) * lit(86400000L))).as("creationDate"),
      lit(deletion).as("deletionDate"),
      lit(false).as("explicitlyDeleted"),
      col("src").as("p1"),
      (lit(base) + pmod(cur, lit(nodes))).as("p2")
    ).where(col("p1") =!= col("p2"))
      .select(
        col("creationDate"), col("deletionDate"), col("explicitlyDeleted"),
        least(col("p1"), col("p2")).as("Person1Id"),
        greatest(col("p1"), col("p2")).as("Person2Id")
      )
      .distinct()
    out = replace(out, entityType(out, "Person -[Knows]-> Person"),
      knows.unionByName(rmatEdges.select(knows.columns.map(col): _*)))
    log.info(s"enhance.TopologySkew: injected R-MAT subgraph ($nodes nodes, ~${nodes * edgesPerNode} edges, base $base)")
    out
  }

  // ---- fragmentation & asymmetry ------------------------------------------

  /** Drops a hash-selected fraction of knows edges, breaking the graph into fragments. */
  private def withFragmentation(g: RawGraph, cfg: EnhanceConfig): RawGraph = {
    val name = "Person -[Knows]-> Person"
    val (_, knows) = entity(g, name)
      .getOrElse(throw new IllegalStateException(s"enhance.TopologySkew: $name entity missing"))
    val kept = knows.where(Hashes.hash01(col("Person1Id"), col("Person2Id"), lit(cfg.salt)) >= lit(cfg.fragmentDropKnowsRatio))
    log.info(s"enhance.TopologySkew: fragmentation keeps hash01 >= ${cfg.fragmentDropKnowsRatio} of knows edges")
    replace(g, entityType(g, name), kept)
  }

  /** Appends flipped copies of a hash-selected fraction of knows edges (P1 > P2 rows appear). */
  private def withAsymmetry(g: RawGraph, cfg: EnhanceConfig): RawGraph = {
    val name = "Person -[Knows]-> Person"
    val (_, knows) = entity(g, name)
      .getOrElse(throw new IllegalStateException(s"enhance.TopologySkew: $name entity missing"))
    val flipped = knows
      .where(Hashes.chance(col("Person1Id"), col("Person2Id"), lit("flip"), lit(cfg.salt))(cfg.unidirFlipRatio))
      .select(
        col("creationDate"), col("deletionDate"), col("explicitlyDeleted"),
        col("Person2Id").as("Person1Id"), col("Person1Id").as("Person2Id")
      )
    log.info(s"enhance.TopologySkew: appended flipped knows copies (ratio ${cfg.unidirFlipRatio})")
    replace(g, entityType(g, name), knows.unionByName(flipped.select(knows.columns.map(col): _*)))
  }

  private def entityType(g: RawGraph, name: String) =
    entity(g, name).getOrElse(throw new IllegalStateException(s"enhance.TopologySkew: $name entity missing"))._1
}

/** F4: zipf attribute skew – rewrites attribute values so their distribution follows a Zipf law. */
object ZipfSkew extends Logging {

  import org.apache.spark.sql.functions.broadcast

  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = {
    if (cfg.zipfAttrs.isEmpty) {
      log.warn("enhance.ZipfSkew: enabled but no attributes configured (enhance.zipf.attrs)")
      g
    } else {
      cfg.zipfAttrs.toSeq.sorted.foldLeft(g)((graph, attr) => applyAttr(graph, attr, cfg))
    }
  }

  private def applyAttr(g: RawGraph, attr: String, cfg: EnhanceConfig): RawGraph = {
    val updates = for {
      (tpe, df) <- g.entities.toSeq
      if df.columns.contains(attr)
    } yield tpe -> zipfColumn(df, attr, cfg)
    log.info(s"enhance.ZipfSkew: applied zipf skew to attribute '$attr' on ${updates.size} entities")
    replaceAll(g, updates)
  }

  /**
    * Replaces `attr` with a value drawn from the original distinct set under a
    * Zipf (rank+1)^-s law. The value set is materialized on the driver (low
    * cardinality attributes), bucketed into an equal-width bucket -> value
    * table and broadcast-joined back on a hash of the entity id, keeping the
    * main dataflow shuffle-free and fully deterministic.
    */
  private def zipfColumn(df: DataFrame, attr: String, cfg: EnhanceConfig): DataFrame = {
    val spark = df.sparkSession
    import spark.implicits._
    val values = df.select(col(attr)).where(col(attr).isNotNull).distinct().as[String].collect().sorted
    if (values.length <= 1) {
      log.info(s"enhance.ZipfSkew: '$attr' has ${values.length} distinct value(s); skipped")
      return df
    }
    val n        = values.length
    val bktCount = math.min(math.max(n * 2L, 16L), cfg.zipfBuckets).toInt
    // prefix sums of the zipf weights w(rank) = 1 / (rank+1)^s
    val cum = new Array[Double](n)
    var acc = 0.0
    for (i <- 0 until n) { acc += 1.0 / math.pow(i + 1, cfg.zipfS); cum(i) = acc }
    val total = acc
    val rows = (0 until bktCount).iterator.map { b =>
      val u = (b + 0.5) / bktCount.toDouble * total
      val idx = java.util.Arrays.binarySearch(cum, u) match {
        case x if x >= 0 => x               // exact prefix-sum hit
        case x           => math.min(-x - 1, n - 1) // first prefix sum > u
      }
      (b.toLong, values(idx))
    }.toSeq
    val bucketDf  = spark.createDataFrame(rows).toDF("bkt", "zv")
    val bkt = least(
      floor(Hashes.hash01(col("id"), lit(s"$attr-zipf"), lit(cfg.salt)) * lit(bktCount.toDouble)),
      lit(bktCount - 1L)
    )
    df.join(broadcast(bucketDf), bkt === col("bkt"), "left")
      .select(df.columns.map {
        case `attr` => when(col(attr).isNotNull, col("zv")).otherwise(col(attr)).as(attr)
        case other  => col(other)
      }: _*)
  }
}

/** F5: temporal burst windows and hot/cold data split. */
object TemporalBurst extends Logging {

  import org.apache.spark.sql.functions.broadcast

  private val DayMillis = 86400000L

  private val Messages = Seq("Post", "Comment")

  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = {
    if (!cfg.burstEnabled && !cfg.hotcoldSplit && !cfg.supernodeLikeBurstEnabled) g
    else {
      val g1 =
        if (cfg.burstEnabled || cfg.hotcoldSplit)
          Messages.foldLeft(g)((graph, name) => burstMessages(graph, name, cfg))
        else g
      val g2 = if (cfg.supernodeLikeBurstEnabled) burstSupernodeLikes(g1, cfg) else g1
      log.info(s"enhance.TemporalBurst: applied (burst=${cfg.burstEnabled} hotcoldSplit=${cfg.hotcoldSplit} " +
        s"supernodeLikeBurst=${cfg.supernodeLikeBurstEnabled})")
      g2
    }
  }

  /**
    * Burst-window creation date for an entity id: window `bw` of the last
    * `recentDays` days (split into `windowCount` windows), offset `bo` inside
    * the window, clamped to the simulation end.
    */
  private def burstCdExpr(id: Column, end0: Long, cfg: EnhanceConfig): Column = {
    val recentSpan = cfg.burstRecentDays.toLong * DayMillis
    val step       = math.max(1L, recentSpan / math.max(1, cfg.burstWindowCount.toLong))
    val winStart   = lit(end0 - recentSpan) +
      Hashes.hashMod(id, lit("bw"), lit(cfg.salt))(cfg.burstWindowCount) * lit(step)
    least(winStart + Hashes.hashMod(id, lit("bo"), lit(cfg.salt))(cfg.burstWindowMillis), lit(end0))
  }

  /**
    * Re-dates Post/Comment creation times:
    *  - hotcoldSplit: a `hotcoldHotRatio` hash sample is compressed into the
    *    last 10% of the entity's [min creation, max deletion] span;
    *  - burstEnabled: a `burstRatio` hash sample is re-dated into the burst
    *    windows inside the last `recentDays` days.
    * Burst is applied after hot/cold, so burst wins on overlap. Non-selected
    * rows keep their original dates untouched; deletionDate is only pushed
    * forward (greatest) so it always stays >= creationDate + 1 day.
    */
  private def burstMessages(g: RawGraph, name: String, cfg: EnhanceConfig): RawGraph = {
    val (tpe, df) = entity(g, name)
      .getOrElse(throw new IllegalStateException(s"enhance.TemporalBurst: $name entity missing"))
    val bounds = df.agg(min(col("creationDate")), max(col("deletionDate"))).head()
    val start0 = bounds.getLong(0)
    val end0   = bounds.getLong(1)
    val id     = col("id")

    var newCd = col("creationDate")
    var newDd = col("deletionDate")
    if (cfg.hotcoldSplit) {
      val span    = math.max(end0 - start0, DayMillis)
      val hotSpan = math.max(span / 10, 1L)
      val hotCd   = lit(end0 - hotSpan) + Hashes.hashMod(id, lit("ho"), lit(cfg.salt))(hotSpan)
      val isHot   = Hashes.hash01(id, lit("hotcold"), lit(cfg.salt)) < lit(cfg.hotcoldHotRatio)
      newCd = when(isHot, hotCd).otherwise(newCd)
      newDd = when(isHot, greatest(newDd, hotCd + lit(DayMillis))).otherwise(newDd)
    }
    if (cfg.burstEnabled) {
      val bCd   = burstCdExpr(id, end0, cfg)
      val isBur = Hashes.hash01(id, lit("burst"), lit(cfg.salt)) < lit(cfg.burstRatio)
      newCd = when(isBur, bCd).otherwise(newCd)
      newDd = when(isBur, greatest(newDd, bCd + lit(DayMillis))).otherwise(newDd)
    }

    val out = df.select(df.columns.map {
      case "creationDate" => newCd.as("creationDate")
      case "deletionDate" => newDd.as("deletionDate")
      case other          => col(other)
    }: _*)
    log.info(s"enhance.TemporalBurst: $name re-dated (data span [$start0, $end0])")
    replace(g, tpe, out)
  }

  /**
    * Likes on hot posts (posts created by supernodes) are re-dated into the
    * burst windows, modelling like storms arriving in bursts. The supernode
    * set is recomputed with the same deterministic formula as TopologySkew,
    * so the result does not depend on whether TopologySkew ran. Non-hot rows
    * are untouched (left join + isNotNull guard).
    */
  private def burstSupernodeLikes(g: RawGraph, cfg: EnhanceConfig): RawGraph = {
    val name = "Person -[Likes]-> Post"
    val (tpe, likes) = entity(g, name)
      .getOrElse(throw new IllegalStateException(s"enhance.TemporalBurst: $name entity missing"))
    val (_, posts) = entity(g, "Post")
      .getOrElse(throw new IllegalStateException("enhance.TemporalBurst: Post entity missing"))
    val (_, persons) = entity(g, "Person")
      .getOrElse(throw new IllegalStateException("enhance.TemporalBurst: Person entity missing"))

    val ctx = IdRemap.ctxOf(cfg)
    val s = persons
      .where(col("id") < lit(ctx.m))
      .where(Hashes.hashMod(col("id"), lit(cfg.salt))(10000) < lit(cfg.supernodeRate * 10000))
      .select(col("id").as("sid"))

    val end0 = posts.agg(max(col("deletionDate"))).head().getLong(0)
    val hotPosts = posts.join(broadcast(s), col("CreatorPersonId") === col("sid"))
      .select(col("id").as("hpid"))
    val bCd   = burstCdExpr(col("PostId"), end0, cfg)
    val isHot = col("hpid").isNotNull
    val out = likes.join(broadcast(hotPosts), col("PostId") === col("hpid"), "left")
      .select(likes.columns.map {
        case "creationDate" => when(isHot, bCd).otherwise(col("creationDate")).as("creationDate")
        case "deletionDate" =>
          when(isHot, greatest(col("deletionDate"), bCd + lit(DayMillis)))
            .otherwise(col("deletionDate")).as("deletionDate")
        case other => col(other)
      }: _*)
    log.info("enhance.TemporalBurst: bursted likes on supernode posts")
    replace(g, tpe, out)
  }
}

/** F6: dirty data injection (nulls, self-loops, duplicates, dangling FKs, invalid values). */
object DirtyData extends Logging {
  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = g
}
