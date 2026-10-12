package ldbc.snb.datagen.enhance

/**
  * Configuration for the skew-enhancement modules.
  *
  * All knobs are prefixed with `enhance.` and parsed from a key=value map
  * (CLI `--enhance-params` and/or `--enhance-param-file`). Everything
  * defaults to disabled, in which case the generator behaves exactly
  * like the native LDBC SNB Datagen.
  *
  * @param enabled master switch; when false the transform is the identity
  */
case class EnhanceConfig(
    enabled: Boolean = false,
    // ---- F2: partition skew (deterministic bijective id remap) ----
    remapEnabled: Boolean = false,
    remapHotEvery: Long = 20,
    // ---- F3: topology skew ----
    supernodeEnabled: Boolean = false,
    supernodeRate: Double = 0.0001,
    supernodeMinExtra: Long = 100,
    supernodeMaxExtraDegreeMul: Long = 10000,
    hotmsgEnabled: Boolean = false,
    hotmsgLikes: Long = 1000,
    hotmsgReplies: Long = 50,
    hotmsgTags: Long = 8,
    smallcompEnabled: Boolean = false,
    smallcompCount: Long = 1000,
    smallcompCliqueMax: Int = 5,
    smallcompReserveBase: Long = 1000000000000L,
    fragmentDropKnowsRatio: Double = 0.0,
    unidirFlipRatio: Double = 0.0,
    rmatEnabled: Boolean = false,
    rmatNodes: Long = 100000,
    rmatEdgesPerNode: Double = 8.0,
    rmatA: Double = 0.57,
    rmatB: Double = 0.19,
    rmatC: Double = 0.19,
    // ---- F4: zipf attribute skew ----
    zipfEnabled: Boolean = false,
    zipfS: Double = 1.1,
    zipfAttrs: Set[String] = Set.empty,
    zipfBuckets: Long = 1000000,
    // ---- F5: temporal burst ----
    burstEnabled: Boolean = false,
    burstRecentDays: Int = 30,
    burstRatio: Double = 0.2,
    burstWindowMillis: Long = 3600000L,
    burstWindowCount: Int = 24,
    supernodeLikeBurstEnabled: Boolean = false,
    hotcoldSplit: Boolean = false,
    hotcoldHotRatio: Double = 0.95,
    // ---- F6: dirty data ----
    dirtyNullContentRatio: Double = 0.0,
    dirtyEmptyContentRatio: Double = 0.0,
    dirtySelfLoopRatio: Double = 0.0,
    dirtyDupEdgeRatio: Double = 0.0,
    dirtyDanglingLikeRatio: Double = 0.0,
    dirtyInvalidIpRatio: Double = 0.0,
    dirtyOutOfRangeDateRatio: Double = 0.0,
    // deterministic seed for hash-based pseudo randomization
    salt: Long = 0x5eedL
)

object EnhanceConfig {

  private sealed trait ValType
  private case object TBool extends ValType
  private case object TInt  extends ValType
  private case object TReal extends ValType
  private case object TStr  extends ValType

  private type Setter = (EnhanceConfig, Any) => EnhanceConfig
  private case class Setting(key: String, tpe: ValType, setter: Setter)

  private val spec: Map[String, Setting] = {
    def b(f: (EnhanceConfig, Boolean) => EnhanceConfig): Setter =
      (c, v) => f(c, v.asInstanceOf[Boolean])
    def i(f: (EnhanceConfig, Long) => EnhanceConfig): Setter =
      (c, v) => f(c, v.asInstanceOf[Long])
    def r(f: (EnhanceConfig, Double) => EnhanceConfig): Setter =
      (c, v) => f(c, v.asInstanceOf[Double])
    def s(f: (EnhanceConfig, String) => EnhanceConfig): Setter =
      (c, v) => f(c, v.asInstanceOf[String])

    val settings = Seq[Setting](
      Setting("enhance.enabled", TBool, b((c, v) => c.copy(enabled = v))),
      // F2: partition skew
      Setting("enhance.remap.enabled", TBool, b((c, v) => c.copy(remapEnabled = v))),
      Setting("enhance.remap.hotEvery", TInt, i((c, v) => c.copy(remapHotEvery = v))),
      // F3: topology skew
      Setting("enhance.supernode.enabled", TBool, b((c, v) => c.copy(supernodeEnabled = v))),
      Setting("enhance.supernode.rate", TReal, r((c, v) => c.copy(supernodeRate = v))),
      Setting("enhance.supernode.minExtra", TInt, i((c, v) => c.copy(supernodeMinExtra = v))),
      Setting("enhance.supernode.maxExtraDegreeMul", TInt, i((c, v) => c.copy(supernodeMaxExtraDegreeMul = v))),
      Setting("enhance.hotmsg.enabled", TBool, b((c, v) => c.copy(hotmsgEnabled = v))),
      Setting("enhance.hotmsg.likes", TInt, i((c, v) => c.copy(hotmsgLikes = v))),
      Setting("enhance.hotmsg.replies", TInt, i((c, v) => c.copy(hotmsgReplies = v))),
      Setting("enhance.hotmsg.tags", TInt, i((c, v) => c.copy(hotmsgTags = v))),
      Setting("enhance.smallcomp.enabled", TBool, b((c, v) => c.copy(smallcompEnabled = v))),
      Setting("enhance.smallcomp.count", TInt, i((c, v) => c.copy(smallcompCount = v))),
      Setting("enhance.smallcomp.cliqueMax", TInt, i((c, v) => c.copy(smallcompCliqueMax = v.toInt))),
      Setting("enhance.smallcomp.reserveBase", TInt, i((c, v) => c.copy(smallcompReserveBase = v))),
      Setting("enhance.fragment.dropKnowsRatio", TReal, r((c, v) => c.copy(fragmentDropKnowsRatio = v))),
      Setting("enhance.unidir.flipRatio", TReal, r((c, v) => c.copy(unidirFlipRatio = v))),
      Setting("enhance.rmat.enabled", TBool, b((c, v) => c.copy(rmatEnabled = v))),
      Setting("enhance.rmat.nodes", TInt, i((c, v) => c.copy(rmatNodes = v))),
      Setting("enhance.rmat.edgesPerNode", TReal, r((c, v) => c.copy(rmatEdgesPerNode = v))),
      Setting("enhance.rmat.a", TReal, r((c, v) => c.copy(rmatA = v))),
      Setting("enhance.rmat.b", TReal, r((c, v) => c.copy(rmatB = v))),
      Setting("enhance.rmat.c", TReal, r((c, v) => c.copy(rmatC = v))),
      // F4: zipf attribute skew
      Setting("enhance.zipf.enabled", TBool, b((c, v) => c.copy(zipfEnabled = v))),
      Setting("enhance.zipf.s", TReal, r((c, v) => c.copy(zipfS = v))),
      Setting("enhance.zipf.attrs", TStr, s((c, v) => c.copy(zipfAttrs = v.split(",").map(_.trim).filter(_.nonEmpty).toSet))),
      Setting("enhance.zipf.buckets", TInt, i((c, v) => c.copy(zipfBuckets = v))),
      // F5: temporal burst
      Setting("enhance.burst.enabled", TBool, b((c, v) => c.copy(burstEnabled = v))),
      Setting("enhance.burst.recentDays", TInt, i((c, v) => c.copy(burstRecentDays = v.toInt))),
      Setting("enhance.burst.ratio", TReal, r((c, v) => c.copy(burstRatio = v))),
      Setting("enhance.burst.windowMillis", TInt, i((c, v) => c.copy(burstWindowMillis = v))),
      Setting("enhance.burst.windowCount", TInt, i((c, v) => c.copy(burstWindowCount = v.toInt))),
      Setting("enhance.supernodeLikeBurst.enabled", TBool, b((c, v) => c.copy(supernodeLikeBurstEnabled = v))),
      Setting("enhance.hotcold.split", TBool, b((c, v) => c.copy(hotcoldSplit = v))),
      Setting("enhance.hotcold.hotRatio", TReal, r((c, v) => c.copy(hotcoldHotRatio = v))),
      // F6: dirty data
      Setting("enhance.dirty.nullContentRatio", TReal, r((c, v) => c.copy(dirtyNullContentRatio = v))),
      Setting("enhance.dirty.emptyContentRatio", TReal, r((c, v) => c.copy(dirtyEmptyContentRatio = v))),
      Setting("enhance.dirty.selfLoopRatio", TReal, r((c, v) => c.copy(dirtySelfLoopRatio = v))),
      Setting("enhance.dirty.dupEdgeRatio", TReal, r((c, v) => c.copy(dirtyDupEdgeRatio = v))),
      Setting("enhance.dirty.danglingLikeRatio", TReal, r((c, v) => c.copy(dirtyDanglingLikeRatio = v))),
      Setting("enhance.dirty.invalidIpRatio", TReal, r((c, v) => c.copy(dirtyInvalidIpRatio = v))),
      Setting("enhance.dirty.outOfRangeDateRatio", TReal, r((c, v) => c.copy(dirtyOutOfRangeDateRatio = v))),
      Setting("enhance.salt", TInt, i((c, v) => c.copy(salt = v)))
    )
    settings.map(st => st.key -> st).toMap
  }

  private def parseBool(v: String, key: String): Either[String, Boolean] = v.toLowerCase match {
    case "true" | "1" | "yes" | "on"  => Right(true)
    case "false" | "0" | "no" | "off" => Right(false)
    case _                            => Left(s"$key: expected boolean, got '$v'")
  }

  private def parseLong(v: String, key: String): Either[String, Long] =
    try Right(java.lang.Long.parseLong(v.trim, 10))
    catch { case _: NumberFormatException => Left(s"$key: expected integer, got '$v'") }

  private def parseDouble(v: String, key: String): Either[String, Double] =
    try Right(java.lang.Double.parseDouble(v.trim))
    catch { case _: NumberFormatException => Left(s"$key: expected number, got '$v'") }

  private def checkRange(key: String, v: Double): Either[String, Unit] = key match {
    case k if k.endsWith("Ratio") || k == "enhance.supernode.rate" =>
      if (v < 0.0 || v > 1.0) Left(s"$key: value $v out of range [0, 1]") else Right(())
    case "enhance.remap.hotEvery" =>
      if (v < 2 || v > 1e9) Left(s"$key: value $v out of range [2, 1e9]") else Right(())
    case "enhance.zipf.s" =>
      if (v < 0.01 || v > 10.0) Left(s"$key: value $v out of range [0.01, 10]") else Right(())
    case _ => Right(())
  }

  /**
    * Parses a flat key=value map into an [[EnhanceConfig]].
    * Keys must carry the `enhance.` prefix; unknown keys are rejected to catch typos.
    */
  def parse(params: Map[String, String]): Either[String, EnhanceConfig] = {
    var config = EnhanceConfig()
    val errors = scala.collection.mutable.ArrayBuffer.empty[String]
    for { (rawKey, rawValue) <- params } {
      val key   = rawKey.trim
      val value = rawValue.trim
      if (!key.startsWith("enhance.")) {
        errors += s"unexpected key '$key' (enhance parameters must be prefixed with 'enhance.')"
      } else spec.get(key) match {
        case None => errors += s"unknown enhance parameter '$key'"
        case Some(Setting(_, tpe, setter)) =>
          val parsed: Either[String, Any] = tpe match {
            case TBool => parseBool(value, key)
            case TInt  => parseLong(value, key).flatMap(l => checkRange(key, l.toDouble).map(_ => l))
            case TReal => parseDouble(value, key).flatMap(d => checkRange(key, d).map(_ => d))
            case TStr  => Right(value)
          }
          parsed match {
            case Right(v)  => config = setter(config, v)
            case Left(err) => errors += err
          }
      }
    }
    if (errors.nonEmpty) Left(errors.mkString("; ")) else Right(config)
  }

  /** Reads an INI-style parameter file (key=value or key:value, '#' comments). */
  def parseFile(path: String): Either[String, Map[String, String]] =
    try {
      val src = scala.io.Source.fromFile(path)
      try {
        Right(src
          .getLines()
          .map(_.trim)
          .filter(l => l.nonEmpty && !l.startsWith("#") && !l.startsWith("["))
          .map { l =>
            val sep = if (l.contains('=')) '=' else ':'
            val idx = l.indexOf(sep)
            (l.substring(0, idx).trim, l.substring(idx + 1).trim)
          }
          .toMap)
      } finally src.close()
    } catch {
      case e: Exception => Left(s"cannot read enhance param file '$path': ${e.getMessage}")
    }

  /** Loads config: file first, then CLI params (CLI takes precedence). */
  def load(paramFile: Option[String], params: Map[String, String]): Either[String, EnhanceConfig] =
    paramFile match {
      case Some(f) => parseFile(f).flatMap(fileParams => parse(fileParams ++ params))
      case None    => parse(params)
    }
}
