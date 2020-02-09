import Util._

object DegreeHeuristicsPregelTest {
  def main(args: Array[String]): Unit = {
    val filteredGraph = get_filtered_graph()
    val connected_components = subgraphs_from_connected_components(filteredGraph).sortBy(c => -c.size)
    val biggest_component_100k = connected_components(0).map(v => v.toDouble).toList.take(100000)
    val graph_100k = filteredGraph.subgraph(e => biggest_component_100k.contains(e.srcId) && biggest_component_100k.contains(e.dstId), (v, _) => biggest_component_100k.contains(v))
    val src_id = graph_100k.vertices.collect()(0)._1.toInt

    val ground_truth = shortest_path_pregel(graph_100k, src_id).groupBy(e => e._2._2.length)

    for (i <- 3 until 10) {
      ground_truth.foreach(chunk => {
        if (chunk._1 != 0) {
          println("Shortest paths of lengths " + chunk._1 + ":")
          val start = System.nanoTime()
          val annotated_graph = heuristic_sssp_pregel(graph_100k, src_id, i)
          println("Runtime ("+ i + " neighbors): " + (System.nanoTime() - start) / 1000 / 1000 + "ms")
        }
      })
    }
  }
}
