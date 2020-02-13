import Util._
import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object DegreeHeuristicsPregelTest {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()

    val Ed = filtered_graph.edges.collect().filter(e => e.srcId == 1).take(1000)
    val Edr = filtered_graph.reverse.edges.collect().filter(e => e.dstId == 1).take(1000)

    print("Edges from id 1: ", Ed.mkString("\n"), "Edges from id 1 reversed", Edr.mkString("\n"))
    val size = filtered_graph.vertices.collect().length
    val r = scala.util.Random

    var errors = new mutable.HashMap[Int, ListBuffer[Int]]
    var interesting_nodes = List[(VertexId, (Double, List[VertexId]))]()
    var nr_interesting_nodes = 0
    var src_id = -1
    var not_found_paths = 0
    val core = spark.read.json(dataDir + "/core_degrees/core_degrees.json").toDF()
    val core_ids = core.select("src").distinct().collect().toList.map(r => r.getLong(0).toInt)

    //Search for a node that has a reasonable connection
    do {
      src_id = filtered_graph.vertices.collect()(math.abs(r.nextInt() % size))._1.toInt //random node
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

    for (nr_neighbors <- 20 to 1 by -1) {
      pathlengths.foreach(pathlength => {
        println(s"Sample of pathlength $pathlength")
        val group = interesting_node_groups(pathlength)
        val sample = group.take(10)
        not_found_paths = 0
        for (i <- 0 until math.min(9, sample.length-1)) {
          val inEdgesDst = filtered_graph.edges.collect().filter(e => e.dstId == sample(i)._1)
          println(s"Searching for path to id ${sample(i)._1} with nr InEdges ${inEdgesDst.length} (Optimal path: ${sample(i)._2._2.toString()})")
          val start = System.nanoTime()
          val prediction = heuristic_sssp_pregel(filtered_graph, src_id, sample(i)._1.toInt, nr_neighbors, core_ids)
          println("Heuristics Runtime (" + nr_neighbors + " neighbors): " + (System.nanoTime() - start) / 1000 / 1000 + "ms")
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
