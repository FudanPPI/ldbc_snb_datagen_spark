package ldbc.snb.datagen.enhance

import ldbc.snb.datagen.util.Logging

/**
  * Enhancement modules. Each module takes the raw IR graph and the config and
  * returns an updated graph. A module whose enabling knobs are all off must
  * be the identity.
  */
/** F2: deterministic bijective person-id remap concentrating hot entities into one partition band. */
object IdRemap extends Logging {
  def transform(g: RawGraph, cfg: EnhanceConfig): RawGraph = g
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
