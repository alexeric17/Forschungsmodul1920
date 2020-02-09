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

      val src_id = graph_100k.vertices.collect()(math.abs(r.nextInt() % 100000))._1.toInt //random node
      val ground_truth = shortest_path_pregel(graph_100k, src_id)
        .map(v => (v._1, v._2)).toMap

      val interesting_nodes = ground_truth.filter(gt => gt._2._2.nonEmpty)
      println("Found " + interesting_nodes.toArray.length + " interesting paths (length>0)")

      val start = System.nanoTime()
      val prediction = heuristic_sssp_pregel(graph_100k, src_id, nr_neighbors)
      println("Heuristics Runtime (" + nr_neighbors + " neighbors): " + (System.nanoTime() - start) / 1000 / 1000 + "ms")
      val prediction_map = prediction.filter(n => interesting_nodes.contains(n._1)).map(v => (v._1, v._2)).toMap
      val cleaned_prediction_map = prediction_map.filter(v => v._2._2.nonEmpty)

      var error = 0
      interesting_nodes.filter(n => cleaned_prediction_map.contains(n._1)).foreach(n => {
        val pathlength = n._2._2.length
        val diff = math.abs(cleaned_prediction_map(n._1)._2.length - pathlength)
        if (!errors.contains(pathlength)) {
          errors(pathlength) = ListBuffer(pathlength)
        } else {
          errors(pathlength) += diff
        }
      })
      println("Neighborhood size: " + nr_neighbors)
      println("Nr of not found paths: " + (prediction_map.toArray.length - cleaned_prediction_map.toArray.length))
      var total_nr = 0
      var total_error = 0
      errors.map(e => (e._1, e._2.toList.groupBy(identity).mapValues(_.size))).foreach(pair => {
        println("Paths of length " + pair._1)
        var nr = 0
        var error = 0
        pair._2.foreach(v => {
          println(s"Difference ${v._1}: ${v._2} occurrences")
          nr += v._2
          error += v._2 * v._1
        })
        println(s"Average error on paths of length ${pair._1}: ${error.toFloat / nr}")
        total_nr += nr
        total_error += error
      })
      println(s"Average error for neighborhood size $nr_neighbors: ${total_error.toFloat / total_nr}")
    }
  }
}
