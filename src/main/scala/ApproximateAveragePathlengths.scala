import Util._
import org.apache.spark.graphx.VertexId
import org.apache.spark.graphx.lib.ShortestPaths

import scala.collection.mutable.ListBuffer

object ApproximateAveragePathlengths {
  def main(args: Array[String]): Unit = {


    //Graph
    val filtered_graph = get_filtered_graph()

    //Get biggest connected components
    val bigConnectedComponent = subgraphs_from_connected_components(filtered_graph)(0)
    val subGraph = create_subgraph_from_cc(filtered_graph,bigConnectedComponent)
    println("SubGraph done")

    //Get verticies.
    val verticies = subGraph.vertices.collect()
    val subGraphVerticies = verticies.map(x => x._1.toInt).toList
    val size = verticies.length
    println("Number of verticies in subGraph ", size)

    //Collect 1000 random nodes from subGraph.
    val randomNodesBuffer = ListBuffer[VertexId]()
    val r = scala.util.Random
    println("Collecting 1000 random nodes")
    for(n <- 0 until 1000) {
      val r_node_id= verticies(math.abs(r.nextInt() % size))._1.toInt
      randomNodesBuffer.append(r_node_id)
    }
    println("Nodes collected")

    //1000 random nodes from biggest subGraph.
    val randomNodes = randomNodesBuffer.toList
    println("Number of random nodes in list: ", randomNodes.length)


    //Run sssp on filtered full-graph but with nodes from the randomNodes (from subGraph).
    val result = collection.mutable.Map[Int,Int]()

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
