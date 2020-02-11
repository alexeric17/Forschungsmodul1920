import Util._
import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

object DegreeHeuristicsPregelTest {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    val size = filtered_graph.vertices.collect().length
    val r = scala.util.Random

    for (nr_neighbors <- 1 until 20) {
      var errors = new mutable.HashMap[Int, ListBuffer[Int]]
      var interesting_nodes = List[(VertexId, (Double, List[VertexId]))]()
      var nr_interesting_nodes = 0
      var src_id = -1
      var not_found_paths = 0
      val core = spark.read.json(dataDir + "/core_degrees/core_degrees.json").toDF()
      val core_ids = core.select("src").collectAsList().toList.map(_.toString().toInt)


      //Search for a node that has a reasonable connection
      do {
        src_id = filtered_graph.vertices.collect()(math.abs(r.nextInt() % size))._1.toInt //random node
        val ground_truth = shortest_path_pregel(filtered_graph, src_id)

        interesting_nodes = ground_truth.filter(gt => gt._2._2.nonEmpty).map(v => (v._1, v._2)).toList
        nr_interesting_nodes = interesting_nodes.toArray.length
      } while (nr_interesting_nodes < 1000)

      println(s"Found $nr_interesting_nodes interesting paths (length>0) from source id $src_id")

      interesting_nodes.groupBy(v => v._2._2.length).foreach(group => {
        val pathlength = group._1
        val sample = group._2.take(1).head._1.toInt
        not_found_paths = 0
        val start = System.nanoTime()
        val prediction = heuristic_sssp_pregel(filtered_graph, src_id, sample, nr_neighbors, core_ids)
        println("Heuristics Runtime (" + nr_neighbors + " neighbors): " + (System.nanoTime() - start) / 1000 / 1000 + "ms")
        if (prediction.isEmpty) {
          not_found_paths += 1
        } else {
          val diff = math.abs(pathlength - prediction.length)
          if (!errors.contains(pathlength)) {
            errors(pathlength) = ListBuffer(pathlength)
          } else {
            errors(pathlength) += diff
          }
        }
      })

      println("Neighborhood size: " + nr_neighbors)
      println("Nr of not found paths: " + not_found_paths)
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
