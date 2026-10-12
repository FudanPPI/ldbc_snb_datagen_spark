package ldbc.snb.datagen.enhance

import ldbc.snb.datagen.model.{EntityType, Graph, Mode}
import ldbc.snb.datagen.transformation.transform.Transform
import ldbc.snb.datagen.util.Logging

/**
  * Applies the enabled skew-enhancement modules to the raw IR graph.
  *
  * Module order is fixed and meaningful:
  *   1. IdRemap       – bijective id remap for partition skew (must run first so
  *                      subsequent generated edges/date logic operate on remapped ids)
  *   2. TopologySkew  – supernodes, small components, fragmentation, asymmetric
  *                      edges, R-MAT subgraph
  *   3. ZipfSkew      – attribute value long-tail
  *   4. TemporalBurst – bursty write windows, hot/cold split
  *   5. DirtyData     – nulls, self-loops, duplicates, dangling FKs, invalid values
  *                      (runs last so other modules do not "repair" dirty rows)
  *
  * With the master switch off the transform is the identity, preserving
  * native Datagen output byte-for-byte.
  */
case class EnhanceTransform(cfg: EnhanceConfig) extends Transform[Mode.Raw.type, Mode.Raw.type] with Logging {

  override def transform(input: In): Out = {
    if (!cfg.enabled) {
      input
    } else {
      log.info(s"enhance: applying enhancement modules with config: $cfg")
      val g1 = IdRemap.transform(input, cfg)
      val g2 = TopologySkew.transform(g1, cfg)
      val g3 = ZipfSkew.transform(g2, cfg)
      val g4 = TemporalBurst.transform(g3, cfg)
      val g5 = DirtyData.transform(g4, cfg)
      refreshDefinitions(g5)
    }
  }

  /**
    * Modules may introduce nullable columns (e.g. dirty nulls) or change row
    * content; the entity DDL stored in the graph definition must be refreshed
    * so downstream readers use the actual schema (same as ExplodeAttrs does).
    */
  private def refreshDefinitions(g: Graph[Mode.Raw.type]): Graph[Mode.Raw.type] = {
    val updated = g.entities.map { case (tpe: EntityType, df) => tpe -> Some(df.schema.toDDL) }
    g.copy(definition = g.definition.copy(entities = g.definition.entities ++ updated))
  }
}
