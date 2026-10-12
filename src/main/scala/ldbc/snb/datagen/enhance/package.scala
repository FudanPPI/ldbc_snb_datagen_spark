package ldbc.snb.datagen

import ldbc.snb.datagen.model.{EntityType, Graph, Mode}
import org.apache.spark.sql.DataFrame

/** Shared helpers for the enhancement modules. */
package object enhance {

  type RawGraph = Graph[Mode.Raw.type]

  /** Fetches an entity DataFrame from the graph by its display name (e.g. "Person", "Person -[Knows]-> Person"). */
  def entity(g: RawGraph, name: String): Option[(EntityType, DataFrame)] =
    g.entities.collectFirst { case (t, df) if t.toString == name => t -> df }

  /** Replaces the DataFrame of an entity (key type is preserved). */
  def replace(g: RawGraph, tpe: EntityType, df: DataFrame): RawGraph =
    g.copy(entities = g.entities.updated(tpe, df))

  /** Replaces several entities at once. */
  def replaceAll(g: RawGraph, updates: Seq[(EntityType, DataFrame)]): RawGraph =
    g.copy(entities = updates.foldLeft(g.entities)((m, u) => m.updated(u._1, u._2)))
}
