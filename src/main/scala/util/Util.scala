package util

import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame

import scala.collection.mutable.ListBuffer //needed to avoid defining implicit encoders when serializing data to rdd

object Util {
  //set these to the correct paths and names for your project
  val FM1920HOME = "/scratch/Forschungsmodul1920/forschungsmodul1920"
  val dataDir = FM1920HOME + "/data"
  val nodeDir = dataDir + "/nodes"
  val edgeDir = dataDir + "/edges"
  val filteredNodeDir = dataDir + "/filtered_nodes"
  val filteredEdgeDir = dataDir + "/filtered_edges"
  val pagerankDir = dataDir + "/pageranks"
  val betweennessCentralityDir = dataDir + "/betweenness_centrality"

  val nodeFile = nodeDir + "/nodes.json"
  val edgeFile = edgeDir + "/edges.json"

  val filteredNodeFile = filteredNodeDir + "/filtered_nodes.json"
  val filteredEdgeFile = filteredEdgeDir + "/filtered_edges.json"
  val pagerankFile = pagerankDir + "/pageranks.json"
  val betweennessCentralityFile = betweennessCentralityDir + "/betweenness_centrality.json"

  val spark = SparkSession
    .builder()
    .appName("Forschungsmodul1920")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._ //needed to avoid defining implicit encoders when serializing data to rdd


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

  def create_graphxGraph(node: DataFrame, edge: DataFrame): Graph[String, Double] = {

    //1. Create nodesRDD
    val nodes = node.mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[VertexId]("id"), vertexRow.getAs[String]("title")))
    }).rdd

    //2. Create edgeRDD
    val edges = edge.mapPartitions(edgesRow => {
      edgesRow.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("src"), edgeRow.getAs[Long]("dst"), 1.0)
      })
    }).rdd

    //3. Return graph
    Graph(nodes, edges)

  }


  def write_nodes_and_edges_to_disk(nodes: DataFrame, edges: DataFrame): Unit = {
    val timestamp = System.currentTimeMillis().toString
    nodes.coalesce(1)
      .write
      .json(FM1920HOME + "/data/subgraph_nodes/" + timestamp)

    edges.coalesce(1)
      .write
      .json(FM1920HOME + "/data/subgraph_edges/" + timestamp)
  }

  def shortest_path_graphx(graph: Graph[String, Double], landmarks: List[VertexId], src_id: Int): Option[Int] = {
    val result = ShortestPaths.run(graph, landmarks) //Shortest path from all vertices to vertices.
    val shortestPaths = result
      .vertices
      .filter({ case (id, _) => id == src_id })
      .first()
      ._2
      .get(3)

    shortestPaths
  }

  //SSSP but with a list of actual path. If list is empty no path found. Finds the actual path from all nodes to src_id.
  def shortest_path_pregel(graph: Graph[String, Double], src_id: Int): Array[(VertexId, (Double, List[VertexId]))] = {
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

  def most_seen_vertex_sssp_pregel(graph: Graph[String, Double], src_id: Int): Map[VertexId, Int] = {

    //1. Calculate shortestpath to src given the graph G. sssp will be a collection of all vertices.
    val sssp = shortest_path_pregel(graph, src_id)

    //2. Convert from Array to Map.
    val allVertex = sssp.toMap.flatMap(x => x._2._2)

    //4. (K,V) Calculate how many times we have seen each vertex.
    val dictnodesSeen = allVertex.groupBy(x => x).mapValues(_.size)

    //5. Most seen node(s)
    val mostSeenNode = dictnodesSeen.maxBy(x => x._2)

    dictnodesSeen

  }

  def scc_graphx(graph: Graph[String, Double], Iter: Int): Unit = {
    // iter = number of iterations
    val scc = graph.stronglyConnectedComponents(Iter)
    scc.vertices.collect.toMap
  }

  def cc_graphx(graph: Graph[String, Double]): Unit = {
    //1. Connected components
    val cc = graph.connectedComponents()

    //2. Get each subGraph as a list.
    val ccVertices = cc.vertices.collect().toMap
    val subGraphs = ccVertices.groupBy(_._2).mapValues(_.keys)
    //3. Get size of each subGraph and number of subgraphs
    val numberOfSubGraphs = cc.vertices.values.distinct.count
    val subGraphSizes = subGraphs.map(x => (x._1, x._2.size))

  }

  def filter_from_nodes_using_list(nodes: DataFrame): DataFrame = {
    //Specificy where to save file.
    val filterWords = List("User:", "Help:", "Category talk:", "Template talk:", "Help talk:", "Wikipedia:", "Wikipedia talk:",
      "MediaWiki:", "MediaWiki talk:", "MediaWiki", "Template:", "User talk:", "Talk:", "Module:", "List of ")

    def containsUdf = udf((strCol: String) => filterWords.exists(strCol.contains))

    val articles = nodes.filter(!containsUdf(col("title")))
    articles

  }

  def filter_from_edges_using_list(nodes: DataFrame, edge: DataFrame): DataFrame = {

    val filterWords = List("User:", "Help:", "Category talk:", "Template talk:", "Help talk:", "Wikipedia:", "Wikipedia talk:",
      "MediaWiki:", "MediaWiki talk:", "MediaWiki", "Template:", "User talk:", "Talk:", "Module:", "List of ")

    def containsUdf = udf((strCol: String) => filterWords.exists(strCol.contains))

    val users = nodes.filter(containsUdf(col("title")))

    val usersList = users.select("id").collect().map(_ (0)).toList

    val newEdges = edge.filter((!$"src".isin(usersList: _*)) && (!$"dst".isin(usersList: _*)))

    newEdges
  }

  def compute_filtered_graph(): Graph[String, Double] = {
    val nodesDF = spark.read.json(nodeFile)
    val edgesDF = spark.read.json(edgeFile)
    val nodes = filter_from_nodes_using_list(nodesDF).mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[VertexId]("id"), vertexRow.getAs[String]("title")))
    }).rdd
    val edges = filter_from_edges_using_list(nodesDF, edgesDF).mapPartitions(edgesRow => {
      edgesRow.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("src"), edgeRow.getAs[Long]("dst"), 1.0)
      })
    }).rdd
    Graph(nodes, edges)
  }

  def compute_filtered_graphframe(): GraphFrame = {
    val nodesDF = spark.read.json(nodeFile)
    val edgesDF = spark.read.json(edgeFile)
    GraphFrame(filter_from_nodes_using_list(nodesDF), filter_from_edges_using_list(nodesDF, edgesDF))
  }

  def get_graph(): Graph[String, Double] = {
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
    Graph(nodesRDD, edgesRDD)
  }

  def get_graphframe(): GraphFrame = {
    val nodesDF = spark.read.json(nodeFile)
    val edgeDF = spark.read.json(edgeFile)
    GraphFrame(nodesDF, edgeDF)
  }

  def get_filtered_graph(): Graph[String, Double] = {
    val filteredNodesRDD = spark.read.json(filteredNodeFile).mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[VertexId]("id"), vertexRow.getAs[String]("title")))
    }).rdd
    //For each pair (src,dst) in edgeDF, create edge with weight 1. Using graphx Edge function: Edge(srcId,dstId,attr)
    val filteredEdgesRDD = spark.read.json(filteredEdgeFile).mapPartitions(edgesRow => {
      edgesRow.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("src"), edgeRow.getAs[Long]("dst"), 1.0)
      })
    }).rdd

    Graph(filteredNodesRDD, filteredEdgesRDD)
  }

  def get_filtered_graphframe(): GraphFrame = {
    val filteredNodesDF = spark.read.json(filteredNodeFile)
    val filteredEdgesDF = spark.read.json(filteredEdgeFile)
    GraphFrame(filteredNodesDF, filteredEdgesDF)
  }

  def reProdPath(graph: Graph[(Int, Double), Double], srcId: Int, dst: Int): List[Int] = {
    var path = ListBuffer[Int]()

    var currentNode = graph.vertices.filter(x => x._1 == dst).collect().take(1)

    if (currentNode(0)._2._2 != Double.PositiveInfinity) {
      path += currentNode(0)._1.toInt
      while (currentNode(0)._1 != srcId) {
        path += currentNode(0)._1.toInt
        currentNode = graph.vertices.filter(x => x._1 == currentNode(0)._2._1).collect().take(1)
      }
      path += currentNode(0)._1.toInt
    }
    /*
    val seenVertices = graph.vertices.filter(x => x._2._1 != -1).collect()
    println(seenVertices.mkString("\n"))
    */
    path.toList
  }

  def get_core_node_ids(): List[Int] = {
    val core_paths = spark.read.json(dataDir + "/core_degrees/core_degrees.json")

    val core_connection = core_paths
      .toDF()
      .select("src")
      .distinct()
      .map(r => r.getLong(0).toInt)
      .collect()

    core_connection.toList
  }
}