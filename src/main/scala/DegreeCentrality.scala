import Util._
import org.apache.spark.graphx.VertexId

import scala.collection.mutable.ListBuffer

object DegreeCentrality {
  def main(args: Array[String]): Unit = {


    //Graph
    val filtered_graph = get_filtered_graph()

    //Get biggest connected components
    val bigConnectedComponent = subgraphs_from_connected_components(filtered_graph)(0)
    val subGraph = create_subgraph_from_cc(filtered_graph,bigConnectedComponent)
    println("SubGraph done")

    //Get verticies.
    val verticies = subGraph.vertices.collect()
    val subGraphVerticies = verticies.map(x => x._1)
    val size = verticies.length
    println("Number of verticies in subGraph ", size)

    //Collect 1000 random nodes from subGraph.
    var randomNodesBuffer = ListBuffer[Int]()
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
    val path = FM1920HOME + "/data/degreeCentrality/"
    val result = collection.mutable.Map[VertexId,Int]()
    
    var i = 0

    println("Calculating degreeCentrality")
    //For all random nodes in subGraph.
    for(next <- randomNodes){
      //Make sure we keep only nodes from the connected component at each iteration.
      val sssp1 = most_seen_vertex_sssp_pregel(filtered_graph,next)
      //Update result.
      sssp1.foreach(node => result.update(node._1,result.getOrElse(node._1,0) + node._2))
      println(s"Iteration $i\n")
      if((i >= 50) && (i%50 == 0)) {
        val sortedResult = result.toSeq
        println(s"50 nodes at iteration: $i " ,sortedResult.take(50).mkString("\n"))
        spark.sparkContext.parallelize(sortedResult).coalesce(1).saveAsTextFile(path + s"$i")
      }
      i += 1
    }
    spark.sparkContext.parallelize(result.toSeq.filter(x => subGraphVerticies.contains(x._1))).coalesce(1)

    /*
    //TODO create a immutable map which can be sorted. BELOW.
    val resultMap = result.seq
    val sortedResult = resultMap.map(x => x)
    println("DegreeCentrality: ", result)
     */
  }

}
