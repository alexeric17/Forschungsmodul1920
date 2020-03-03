package graph_properties_analysis

import org.apache.spark.graphx.lib.ShortestPaths
import util.Subgraphs._
import util.Util._

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
    for (sample <- randomNodes.grouped(10)) {
      println(s"Looking at sample $i of 100")
      i += 1

      val annotated = ShortestPaths.run(filtered_graph, sample)

      for (v <- annotated.vertices.collect()) {
        for (value <- v._2)
          if (!result.contains(value._2)) {
            result(value._2) = 1
          } else {
            result(value._2) += 1
          }
      }
    }


    for (key <- result.keys) {
      println(s"Pathlength $key: ${result(key)}")
    }
  }

}
