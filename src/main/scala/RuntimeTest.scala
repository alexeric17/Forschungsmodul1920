import Util._

object RuntimeTest {
  def main(args: Array[String]): Unit = {
    val graph_10k = get_subgraph(nodesDF, edgeDF, 10000, take_highest = true)
    val graph_50k = get_subgraph(nodesDF, edgeDF, 50000, take_highest = true)

    //1 Subgraph with 10K nodes
    shortest_path_graphx(graph_10k, List(1), 1)
    shortest_path_pregel(graph_10k, 1)
    //2 Subgraph with 50K nodes
    shortest_path_graphx(graph_50k, List(1), 1)
    shortest_path_pregel(graph_50k, 1)
    //3 Full graph
    shortest_path_graphx(graph, List(1), 1)
    shortest_path_pregel(graph, 1)
  }
}
