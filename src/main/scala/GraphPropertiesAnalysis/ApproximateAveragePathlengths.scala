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

    val annotated_nodes = ShortestPaths.run(filtered_graph, randomNodes).vertices.collect()

    for (v <- annotated_nodes) {
      for (value <- v._2)
        if (!result.contains(value._2)) {
          result(value._2) = 1
        } else {
          result(value._2) += 1
        }
    }

    for (key <- result.keys) {
      println(s"Pathlength $key: ${result(key)}")
    }
  }

}
