package HeuristicsTests

import org.apache.spark.graphx.VertexId
import util.Heuristics._
import util.Util._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object PagerankHeuristicsTest {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    val annotated_graph = pagerankHeuristics(filtered_graph)

    val size = annotated_graph.vertices.collect().length
    val r = scala.util.Random

    var errors = new mutable.HashMap[Int, ListBuffer[Int]]
    var interesting_nodes = List[(VertexId, (Double, List[VertexId]))]()
    var nr_interesting_nodes = 0
    var src_id = -1

    //Search for a node that has a reasonable connection
    do {
      src_id = annotated_graph.vertices.collect()(math.abs(r.nextInt() % size))._1.toInt //random node
      val ground_truth = shortest_path_pregel(filtered_graph, src_id)

      interesting_nodes = ground_truth.filter(gt => gt._2._2.nonEmpty).map(v => (v._1, v._2)).toList
      nr_interesting_nodes = interesting_nodes.toArray.length
    } while (nr_interesting_nodes < 1000)

    val interesting_node_groups = interesting_nodes.groupBy(v => v._2._2.length)
    val pathlengths = interesting_node_groups.keys.toList.sortBy(x => x)
    var total_pathlength = 0
    interesting_node_groups.foreach(g => total_pathlength += (g._1 * g._2.length))
    println(s"Found $nr_interesting_nodes interesting paths (length>0) from source id $src_id with the following pathlength nrs:")
    interesting_node_groups.foreach(g => println(s"Length ${g._1}: ${g._2.length} Occurences"))
    println(s"Average Pathlength: ${total_pathlength.toDouble / nr_interesting_nodes}")

    for (nr_neighbors <- 1 to 100) {
      errors.clear()
      var not_found_paths = 0
      var runtimes = ListBuffer[Double]()

      pathlengths.foreach(pathlength => {
        println(s"Samples of pathlength $pathlength :")
        val group = interesting_node_groups(pathlength)
        val sample = group.take(5)
        for (i <- 0 until math.min(9, sample.length - 1)) {
          val inEdgesDst = annotated_graph.edges.collect().filter(e => e.dstId == sample(i)._1)
          val start = System.nanoTime()
          val prediction = heuristics_sssp(annotated_graph, src_id, sample(i)._1.toInt, nr_neighbors, dataDir + "/core_pagerank/core_pagerank.json")
          val runtime = (System.nanoTime() - start) / 1000 / 1000
          runtimes += runtime
          println("Heuristics Runtime (" + nr_neighbors + " neighbors): " + runtime + "ms")
          println(s"Heuristics Prediction: ${prediction.toString()}")
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
        }
      })

      println("Neighborhood size: " + nr_neighbors)
      println("Average runtime: " + (runtimes.sum.toLong / runtimes.length))
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
