import Util._

object ConnectedComponentsAnalysis {
  val TOTAL_NODES = graph.vertices.collect().length
  val TOTAL_EDGES = graph.edges.collect().length

  def main(args: Array[String]): Unit = {
    val components = subgraphs_from_connected_components(graph)
    val subgraphs = create_all_subgraphs_from_cc(graph, components, components.length)

    val ordered_subgraphs = subgraphs.sortBy(g => -g.vertices.collect().length)
    for ((component, i) <- ordered_subgraphs.view.zipWithIndex) {
      val nr_nodes = component.vertices.collect().length.toFloat
      val nr_edges = component.edges.collect().length.toFloat
      val node_percentage = nr_nodes / TOTAL_NODES
      val edge_percentage = nr_edges / TOTAL_EDGES
      println(s"#$i largest Component: $nr_nodes Nodes ($node_percentage %, $nr_edges Edges ($edge_percentage %)")
    }
  }
}
