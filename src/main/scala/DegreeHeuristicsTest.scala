import Util._

object DegreeHeuristicsTest {
  def main(args: Array[String]): Unit = {
    val filteredGraph = get_filtered_graph()
    val connected_components = subgraphs_from_connected_components(filteredGraph).sortBy(c => -c.size)
    val biggest_component = connected_components(0).map(v => v.toDouble).toList
    val graph_25k = filteredGraph.subgraph(e => biggest_component.take(25000).contains(e.srcId) && biggest_component.contains(e.dstId), (v, _) => biggest_component.contains(v))
    val src_id = graph_25k.vertices.collect()(0)._1.toInt

    val ground_truth = shortest_path_pregel(graph_25k, src_id).groupBy(e => e._2._2.length)

    ground_truth.foreach(chunk => {
      if (chunk._1 != 0) {
        println("Shortest paths of lengths " + chunk._1 + ":")
        chunk._2.foreach(v => {
          println(reProdPath(heuristics_shortestPath_pair(graph_25k, src_id, v._1.toInt, 3)._1, src_id, v._1.toInt).toString())
        })
      }
    })
  }
}
