import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeRDD, Graph, VertexId, VertexRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib.ShortestPaths
import Util.FM1920HOME


object GraphxShortestPath {

  def main(args: Array[String]): Unit = {

    //1.  Initialize

    val conf = new SparkConf().setAppName("degreeDF").setMaster("local[*]")
    val sc = new SparkContext(conf)


    val spark = SparkSession
      .builder()
      .appName("degreeDF")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._


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

    //2.  Read the graph using nodes and edges located at data/nodes/nodes.json and data/edges/edges.json
    //    nodeDF should have format |id|title|
    //    edgeDF should have format |src|dst|
    val nodeFile = FM1920HOME + "data/nodes/nodes.json"
    val edgeFile = FM1920HOME + "data/edges/edges.json"


    //    Handle nodes. Change 'fieldnames' below  to actual names in nodeDF.
    val graphNodes = spark.read.json(nodeFile).mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[VertexId]("id"), vertexRow.getAs[String]("title")))
    }).rdd


    //    Handle edges. Change 'fieldnames' below to actual names in edgeDF.
    //    "srcId" and "dstId" from edgeDF
    //    For each pair (src,dst) in edgeDF, create edge with weight 1. Using graphx Edge function: Edge(srcId,dstId,attr)
    val edges = spark.read.json(edgeFile).mapPartitions(edgesRow => {
      edgesRow.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("src"), edgeRow.getAs[Long]("dst"), 1.0)
      })
    }).rdd
    //    EdgeRDD used in Graphx-graph constructor.
    val graphEdges = EdgeRDD.fromEdges(edges)


    //3.  Create graphx graph using nodes and edges
    //    edges on format |src|dst|attr(weight)|
    //    nodes on format |id|title|

    val graph = Graph(graphNodes, graphEdges)

    //4.  GraphxShortestPath
    val useShortestPath = 0
    if (useShortestPath == 1) {
      val result = ShortestPaths.run(graph, Seq(3, 2)) //Shortest path from all vertices to vertices.
      val shortestPath = result
        .vertices
        .filter({ case (id, _) => id == 4 })
        .first()
        ._2
        .get(3)

      println("Shortestpath from 4 to node 3")
      shortestPath.foreach(println)

      println("Shorest path results")
      println(result.vertices.collect.mkString("\n"))
    }

    //5.   Singel source shortest path using Pregel
    val sourceId: VertexId = 4

    //    5.1 Single source shortest path (SSSP) using pregel.
    //    Init graph. All vertices except 'root' have distance infinity.
    val initialGraph = graph.mapVertices((id, _) => if(id == sourceId ) 0.0 else Double.PositiveInfinity)

    val sssp = initialGraph.pregel(Double.PositiveInfinity)((id, dist, newDist) => math.min(dist, newDist),
    triplet => { //send msg
      if(triplet.srcAttr + triplet.attr < triplet.dstAttr) {
      Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))}
      else {
        Iterator.empty
      }
    },
      (a, b) => math.min(a, b) //Merge msg
    )
    println(sssp.vertices.collect.mkString("\n"))





    //    5.2 SSSP but with a list with actual path. If list is empty no path found. Finds the actual path from all nodes to soruceId.
    val initialGraph1 : Graph[(Double, List[VertexId]), Double] =
    //    Init graph. All vertices except 'root' have distance infinity. All vertices except 'root' have empty list. Root has itself in list.
      graph.mapVertices((id, _) => if(id == sourceId ) (0.0,List[VertexId](sourceId)) else (Double.PositiveInfinity, List[VertexId]()))

    val sssp1 = initialGraph1.pregel((Double.PositiveInfinity, List[VertexId]()),Int.MaxValue,EdgeDirection.Out)(
      (id,dist,newDist) => if (dist._1 < newDist._1) dist else newDist,

      triplet => { //send msg
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr ) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr , triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      }, //Merge Message
      (a, b) => if (a._1 < b._1) a else b)
    //    Print paths from all nodes to sourceId 'root'
    println(sssp1.vertices.collect.mkString("\n"))


    //Use filter to select specific path between two nodes.
  }

}
