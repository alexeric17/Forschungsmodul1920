import Util._

object RuntimeTest {

  def compute_runtime(size: Int): Unit = {
    val filtered_nodes = filter_from_nodes_using_list(nodesDF)
    val filtered_edges = filter_from_edges_using_list(nodesDF, edgeDF)
    val graph = get_subgraph(filtered_nodes, filtered_edges, size, take_highest = true)
    val components = subgraphs_from_connected_components(graph)
    val subgraphs = create_all_subgraphs_from_cc(graph, components, components.length)

    println(s"${size / 1000}K: #Components: ${components.length}")
    val sorted_components = subgraphs.sortBy(g => -g.vertices.collect().length)
    var runtime_graphx = 0.0
    var runtime_pregel = 0.0
    for (i <- 0 until 10) {
      val component = sorted_components(i)
      println(s"Size of component $i: ${component.vertices.collect().length}")
      val source_id = component.vertices.collect().take(1)(0)._1
      var start = System.nanoTime()
      shortest_path_graphx(component, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtime_graphx += runtime.toFloat

      start = System.nanoTime()
      shortest_path_pregel(component, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " to pregel")
      runtime_pregel += runtime.toFloat
    }
    val avg_runtime_graphx = runtime_graphx / 10
    val avg_runtime_pregel = runtime_pregel / 10
    println(s"$size, graphx: $avg_runtime_graphx ms")
    println(s"$size, pregel: $avg_runtime_pregel ms")
  }

  def main(args: Array[String]): Unit = {
    compute_runtime(5000)
    compute_runtime(10000)
    compute_runtime(25000)
    compute_runtime(50000)
    compute_runtime(150000)
  }
}
