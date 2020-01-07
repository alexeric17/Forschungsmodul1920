import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeRDD, Graph}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.lib.ShortestPaths



object GraphxShortestPath {
  //set this to the home directory of this project
  val FM1920HOME = ""

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

    //2.  Read the graph using nodes and edges located at data/nodes/fileName and data/edges/fileName
    //    nodeDF should have format |id|title|
    //    edgeDF should have format |src|dst|
    //    Enter filename of nodeFileName.json and edgeFileName.json
    val nodeFileName = "node.json"
    val nodeFile = "data/nodes/" + nodeFileName
    val edgeFileName = "edge.json"
    val edgeFile = "data/edges/" + edgeFileName

    //    NOTES: using RDD for now. Possibility to use DFs in graphx?

    //    Handle nodes. Change 'fieldnames' below  to actual names in nodeDF.
    val graphNodes = spark.read.json(nodeFile).mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[Long]("id"), vertexRow.getAs[String]("title")))
    }).rdd

    //    Handle edges. Change 'fieldnames' below to actual names in edgeDF.
    //    "srcId" and "dstId" from edgeDF
    //    For each pair (src,dst) in edgeDF, create edge with weight 1. Using graphx Edge function: Edge(srcId,dstId,attr)
    val edges = spark.read.json(edgeFile).mapPartitions(edgesRow => {
      edgesRow.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("srcId"), edgeRow.getAs[Long]("dstId"),"1")
      })
    }).rdd
    //    EdgeRDD used in Graphx-graph constructor.
    val graphEdges = EdgeRDD.fromEdges(edges)


    //3.  Create graphx graph using nodes and edges
    //    edges on format |src|dst|attr(weight)|
    //    nodes on format |id|title|

    val graph = Graph(graphNodes,graphEdges)

    //4.  GraphxShortestPath
    val result = ShortestPaths.run(graph,Seq(3,2)) //Shortest path from all vertices to vertices.
    val shortestPath = result
      .vertices
      .filter({case(id, _) => id == 4})
      .first()
      ._2
      .get(3)

    println("Shortestpath from 4 to node 3")
    shortestPath.foreach(println)

    println("Shorest path results")
    println(result.vertices.collect.mkString("\n"))

    //    Saving as file. srcId, dstId, srcAttr (edges)
    //    Not sure if this part is needed.
    val tripletsDF = spark.createDataFrame(graph.triplets.map(triplet => (triplet.srcId,triplet.dstId,triplet.srcAttr))).toDF("srcId","dstId")
    tripletsDF.write.json("data/graphxshortestpath")


    }

}
