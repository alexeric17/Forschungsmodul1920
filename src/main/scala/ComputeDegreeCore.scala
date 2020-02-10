import Util._

object ComputeDegreeCore {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    val connected_components = subgraphs_from_connected_components(filtered_graph).sortBy(c => -c.size)
    val biggest_component = connected_components(0).map(v => v.toDouble).toList
    val filtered_subgraph = filtered_graph.subgraph(e =>
      biggest_component.contains(e.srcId) || biggest_component.contains(e.dstId), (v, _) => biggest_component.contains(v))

    val g_degOut = filtered_subgraph.outerJoinVertices(filtered_subgraph.outDegrees)((_, _, deg) => deg.getOrElse(0))
    val core_nodes = g_degOut.vertices.sortBy(v => -v._2).take(100)

    core_nodes.foreach(v => println(s"ID ${v._1} with degree ${v._2}"))
  }
}
