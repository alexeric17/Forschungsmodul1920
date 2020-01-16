import Util._
import org.apache.spark.graphx.VertexId


object GraphxShortestPath {
  val SOURCE_ID = 1 //change this for your usecase

  def main(args: Array[String]): Unit = {
    //----Test case----
    /*
    val edgess = Array(Edge(1L, 2L,""), Edge(1L, 3L,""), Edge(1L, 4L,""), Edge(3L, 4L,""), Edge(2L, 3L,""), Edge(4L, 2L,""))
    val E = sc.parallelize(edgess).toDF()
      E.coalesce(1).write.json("data/edges")
    val ERDD = EdgeRDD.fromEdges(sc.parallelize(edgess))
    val nodess = Array((1L, "alex"), (2L, "boris"), (3L, "carol"), (4L, "dereq"))
    val N = sc.parallelize(nodess).toDF("id","title")
      N.coalesce(1).write.json("data/nodes")
    val NRDD = sc.parallelize(nodess)
    val nowhere = "nowhere"

    val G = Graph(NRDD, ERDD, nowhere)
     */
    //---Test end----

    //1.  GraphxShortestPath
    val useShortestPath = 0
    if (useShortestPath == 1) {
      println("Shortestpath from 4 to node 3")
      shortest_path_graphx(graph, List(3, 2), SOURCE_ID).foreach(println)
    }

    //2. Single source shortest path using Pregel
    val sourceId: VertexId = SOURCE_ID
    println(shortest_path_pregel(graph, SOURCE_ID).mkString("\n"))
  }

}
