package GraphPropertiesAnalysis

import Util.Util._
import org.apache.spark.graphx.lib.ShortestPaths

object ApproximateAveragePathlengths {
  def main(args: Array[String]): Unit = {


    //Graph
    val filtered_graph = get_filtered_graph()

    //Get biggest connected components
    val bigConnectedComponent = subgraphs_from_connected_components(filtered_graph)(0)
    val subGraph = create_subgraph_from_cc(filtered_graph, bigConnectedComponent)
    println("SubGraph done")

    //Get random sample
    val randomNodes = subGraph
      .vertices
      .takeSample(false, 1000, scala.util.Random.nextLong())
      .map(v => v._1)
      .toList

    println("Nodes collected")

    //1000 random nodes from biggest subGraph.
    println("Number of random nodes in list: ", randomNodes.length)


    //Run sssp on filtered full-graph but with nodes from the randomNodes (from subGraph).
    val result = collection.mutable.Map[Int, Int]()

    var i = 1
    for (node <- randomNodes) {
      println("Iteration $i")
      i+=1
      val annotated = shortest_path_pregel(filtered_graph, node.toInt)

      for (v <- annotated) {
        val pathlength = v._2._2.length
        if (!result.contains(pathlength)) {
          result(pathlength) = 1
        } else {
          result(pathlength) += 1
        }
      }
    }
    for (key <- result.keys) {
      println(s"Pathlength $key: ${result(key).toFloat}")
    }
  }
}
