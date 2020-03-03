package graph_properties_analysis

import util.Subgraphs._
import util.Util._

object ConnectedComponentsAnalysis {
  val filtered_graph = get_filtered_graph()
  val TOTAL_NODES = filtered_graph.vertices.collect().length
  val TOTAL_EDGES = filtered_graph.edges.collect().length

  def main(args: Array[String]): Unit = {
    val components = subgraphs_from_connected_components(filtered_graph)
    val ordered_components = components.sortBy(c => -c.size)
    for ((component, i) <- ordered_components.view.zipWithIndex) {
      val subgraph = create_subgraph_from_cc(filtered_graph, component)
      val nr_nodes = subgraph.vertices.collect().length.toFloat
      val nr_edges = subgraph.edges.collect().length.toFloat
      val node_percentage = nr_nodes / TOTAL_NODES
      val edge_percentage = nr_edges / TOTAL_EDGES
      println(s"#$i largest Component: $nr_nodes Nodes ($node_percentage %, $nr_edges Edges ($edge_percentage %)")
    }
  }
}
