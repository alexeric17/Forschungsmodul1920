import Util._

object DegreeHeuristicsPregelTest {
  def main(args: Array[String]): Unit = {
    val filteredGraph = get_filtered_graph()
    val connected_components = subgraphs_from_connected_components(filteredGraph).sortBy(c => -c.size)
    val biggest_component_100k = connected_components(0).map(v => v.toDouble).toList.take(100000)
    val graph_100k = filteredGraph.subgraph(e => biggest_component_100k.contains(e.srcId) && biggest_component_100k.contains(e.dstId), (v, _) => biggest_component_100k.contains(v))
    val r = scala.util.Random

    for (nr_neighbors <- 3 until 10) {
      for (_ <- 0 until 5) {
        val src_id = graph_100k.vertices.collect()(math.abs(r.nextInt() % 100000))._1.toInt //random node
        val ground_truth = shortest_path_pregel(graph_100k, src_id)
          .map(v => (v._1, v._2)).toMap

        val start = System.nanoTime()
        val prediction = heuristic_sssp_pregel(graph_100k, src_id, nr_neighbors)
        println("Heuristics Runtime ("+ nr_neighbors + " neighbors): " + (System.nanoTime() - start) / 1000 / 1000 + "ms")
        val prediction_map = prediction.map(v => (v._1, v._2)).toMap

        var error = 0
        ground_truth.foreach(e => {
          val diff = e._2._2.length - prediction_map(e._1)._2.length
          println("Adding diff of " + diff + " to current error estimate")
          error += diff
        })
        println("Total error of heuristics ("+ nr_neighbors + " neighbors): " + error)
      }
    }
  }
}
