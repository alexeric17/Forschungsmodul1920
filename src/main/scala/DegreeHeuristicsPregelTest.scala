import Util._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DegreeHeuristicsPregelTest {
  def main(args: Array[String]): Unit = {
    val filteredGraph = get_filtered_graph()
    val connected_components = subgraphs_from_connected_components(filteredGraph).sortBy(c => -c.size)
    val biggest_component_100k = connected_components(0).map(v => v.toDouble).toList.take(100000)
    val graph_100k = filteredGraph.subgraph(e => biggest_component_100k.contains(e.srcId) && biggest_component_100k.contains(e.dstId), (v, _) => biggest_component_100k.contains(v))
    val r = scala.util.Random

    for (nr_neighbors <- 3 until 10) {
      var errors = new mutable.HashMap[Int, ListBuffer[Int]]

      for (_ <- 0 until 5) {
        val src_id = graph_100k.vertices.collect()(math.abs(r.nextInt() % 100000))._1.toInt //random node
        val ground_truth = shortest_path_pregel(graph_100k, src_id)
          .map(v => (v._1, v._2)).toMap

        val interesting_nodes = ground_truth.filter(gt => gt._2._2.nonEmpty)

        val start = System.nanoTime()
        val prediction = heuristic_sssp_pregel(graph_100k, src_id, nr_neighbors)
        println("Heuristics Runtime ("+ nr_neighbors + " neighbors): " + (System.nanoTime() - start) / 1000 / 1000 + "ms")
        val prediction_map = prediction.map(v => (v._1, v._2)).toMap

        var error = 0
        interesting_nodes.foreach(e => {
          val pathlength = e._2._2.length
          val diff = pathlength - prediction_map(e._1)._2.length
          if (!errors.contains(pathlength)) {
            errors(pathlength) = ListBuffer(pathlength)
          } else {
            errors(pathlength) += diff
          }
        })
      }
      println("Neighborhood size: " + nr_neighbors + ", Interesting paths: ")
      errors.map(e => (e._1, e._2.toList.groupBy(identity).mapValues(_.size))).foreach(pair => {
        println("Paths of length " + pair._1)
        var total_nr = 0
        var total_error = 0
        pair._2.foreach(v => {
          println("Difference " + v._1 + ": " + v._2 + " occurences")
          total_nr += v._2
          total_error = v._2 * v._1
        })
        println("Average error: " + total_error.toFloat / total_nr)
      })
    }
  }
}
