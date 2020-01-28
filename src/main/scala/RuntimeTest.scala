import Util._
import org.apache.spark.graphx.Graph

object RuntimeTest {
  val filtered_nodes = filter_from_nodes_using_list(nodesDF)
  val filtered_edges = filter_from_edges_using_list(nodesDF, edgeDF)

  def compute_runtime(size: Int, subgraph: Graph[String, Double]): Unit = {
    var runtime_graphx = 0.0
    var runtime_pregel = 0.0

    println(s"${size / 1000}K:")
    val source_id = subgraph.vertices.collect()(0)._1
    for (i <- 0 until 10) {
      var start = System.nanoTime()
      shortest_path_graphx(subgraph, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtime_graphx += runtime.toFloat

      start = System.nanoTime()
      shortest_path_pregel(subgraph, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to pregel")
      runtime_pregel += runtime.toFloat
    }
    val avg_runtime_graphx = runtime_graphx / 10
    val avg_runtime_pregel = runtime_pregel / 10
    println(s"$size, graphx: $avg_runtime_graphx ms")
    println(s"$size, pregel: $avg_runtime_pregel ms")
  }

  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    val connected_components = subgraphs_from_connected_components(filtered_graph).sortBy(c => c.size)
    val biggest_component = connected_components(0)
    val subgraph = create_subgraph_from_cc(filtered_graph, biggest_component)
    compute_runtime(5000, subgraph)
    compute_runtime(10000, subgraph)
    compute_runtime(25000, subgraph)
    compute_runtime(50000, subgraph)
    compute_runtime(150000, subgraph)
  }
}
