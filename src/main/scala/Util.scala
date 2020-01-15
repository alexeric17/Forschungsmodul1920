import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

object Util {
  //set these to the correct paths and names for your project
  val FM1920HOME = ""
  val nodeDir = FM1920HOME + "/data/nodes"
  val edgeDir = FM1920HOME + "/data/edges"
  val nodeFile = nodeDir + "/nodes.json"
  val edgeFile = edgeDir + "/edges.json"

  val spark = SparkSession
    .builder()
    .appName("Forschungsmodul1920")
    .enableHiveSupport()
    .getOrCreate()

  //For GraphX
  //    nodeFile should have format |id|title|
  //    edgeFile should have format |src|dst|
  val nodesRDD = spark.read.json(nodeFile).mapPartitions(vertices => {
    vertices.map(vertexRow => (vertexRow.getAs[VertexId]("id"), vertexRow.getAs[String]("title")))
  }).rdd
  //For each pair (src,dst) in edgeDF, create edge with weight 1. Using graphx Edge function: Edge(srcId,dstId,attr)
  val edgesRDD = spark.read.json(edgeFile).mapPartitions(edgesRow => {
    edgesRow.map(edgeRow => {
      Edge(edgeRow.getAs[Long]("src"), edgeRow.getAs[Long]("dst"), 1.0)
    })
  }).rdd
  val graph = Graph(nodesRDD, edgesRDD)


  //For GraphFrame
  val nodesDF = spark.read.json(nodeFile)
  val edgeDF = spark.read.json(edgeFile)
  val graphFrame = GraphFrame(nodesDF, edgeDF)


  def create_dict_from_nodes(nodes: DataFrame): collection.Map[Long, String] = {
    nodes
      .rdd
      .map(entry => (entry.getLong(0), entry.getString(1)))
      .collectAsMap()
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, ids: List[Long]): Unit = {
    val timestamp = System.currentTimeMillis().toString
    nodes
      .filter(entry => ids.contains(entry.getLong(0)))
      .distinct()
      .coalesce(1)
      .write
      .json(FM1920HOME + "/data/subgraph_nodes/" + timestamp)

    edges
      .filter(entry => ids.contains(entry.getLong(0)) && ids.contains(entry.getLong(1)))
      .distinct()
      .coalesce(1)
      .write
      .json(FM1920HOME + "/data/subgraph_edges/" + timestamp)
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, min: Int, max: Int): Unit = {
    val list = List
      .range(min, max + 1, 1)
      .map(_.toLong)

    get_subgraph(nodes, edges, list)
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, nr: Int, take_highest: Boolean): Unit = {
    val spark = SparkSession
      .builder()
      .appName("get_subgraph")
      .enableHiveSupport()
      .getOrCreate()

    val degreeDF = spark.read.json(FM1920HOME + "/data/degree/degrees.json")

    val list = if (take_highest) {
      degreeDF
        .orderBy(desc("outEdges"))
        .select("id")
        .take(nr)
        .toList
    } else {
      degreeDF
        .orderBy(asc("outEdges"))
        .select("id")
        .take(nr)
        .toList
    }
    val string_list = list.map(r => r.toString())
    val filtered_string_list = string_list.map(s => s.substring(1, s.length - 1))
    get_subgraph(nodes, edges, filtered_string_list.map(s => s.toLong))
  }

  def shortest_path_graphx(landmarks: Seq[VertexId], src_id: Int): Option[Int] = {
    val result = ShortestPaths.run(graph, landmarks) //Shortest path from all vertices to vertices.
    val shortestPaths = result
      .vertices
      .filter({ case (id, _) => id == src_id })
      .first()
      ._2
      .get(3)

    shortestPaths
  }

  //SSSP but with a list with actual path. If list is empty no path found. Finds the actual path from all nodes to src_id.
  def shortest_path_pregel(src_id: Int): Array[(VertexId, (Double, List[VertexId]))] = {
    val initialGraph1: Graph[(Double, List[VertexId]), Double] =
    //Init graph. All vertices except 'root' have distance infinity. All vertices except 'root' have empty list. Root has itself in list.
      graph.mapVertices((id, _) => if (id == src_id) (0.0, List[VertexId](src_id)) else (Double.PositiveInfinity, List[VertexId]()))

    val sssp1 = initialGraph1.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
      (id, dist, newDist) => if (dist._1 < newDist._1) dist else newDist,

      triplet => { //send msg
        if (triplet.srcAttr._1 < triplet.dstAttr._1 - triplet.attr) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      }, //Merge Message
      (a, b) => if (a._1 < b._1) a else b)

    sssp1.vertices.collect
    //TODO Use filter to select specific path between two nodes.
  }
}
