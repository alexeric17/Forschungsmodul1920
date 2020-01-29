import Util._

object SaveFilteredGraph {
  def main(args: Array[String]): Unit = {
    val filtered_graph = compute_filtered_graphframe()
    filtered_graph.vertices.coalesce(1).write.json(filteredNodeDir)
    filtered_graph.edges.coalesce(1).write.json(filteredEdgeDir)
  }
}
