import Util._

object ConnectedComponentsAnalysis {
  val filtered_graph = get_filtered_graph()
  val TOTAL_NODES = filtered_graph.vertices.collect().length
  val TOTAL_EDGES = filtered_graph.edges.collect().length

  def main(args: Array[String]): Unit = {
    val components = subgraphs_from_connected_components(filtered_graph)
    val subgraphs = create_all_subgraphs_from_cc(filtered_graph, components, components.length)

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
