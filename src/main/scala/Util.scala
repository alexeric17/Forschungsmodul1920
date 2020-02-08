import java.util.Calendar

import org.apache.spark.graphx
import org.apache.spark.graphx.lib.ShortestPaths
import org.apache.spark.graphx.{Edge, EdgeContext, EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.graphframes.GraphFrame
import org.graphframes.lib.ConnectedComponents

import scala.collection.Map

object Util {
  //set these to the correct paths and names for your project
  val FM1920HOME = "/home/alexander/forschungsmodul1920"
  val dataDir = FM1920HOME + "/data"
  val nodeDir = dataDir + "/nodes"
  val edgeDir = dataDir + "/edges"
  /*
  val filteredNodeDir = dataDir + "/filtered_nodes"
  val filteredEdgeDir = dataDir + "/filtered_edges"

   */

  val nodeFile = nodeDir + "/nodes.json"
  val edgeFile = edgeDir + "/edges.json"
/*
  val filteredNodeFile = filteredNodeDir + "/filtered_nodes.json"
  val filteredEdgeFile = filteredEdgeDir + "/filtered_edges.json"

 */



  val spark = SparkSession
    .builder()
    .appName("Forschungsmodul1920")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._ //needed to avoid defining implicit encoders when serializing data to rdd
/*
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
*/

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

  def get_subgraph(nodes: DataFrame, edges: DataFrame, ids: List[Long]): Graph[String, Double] = {
    val filtered_nodes = nodes
      .filter(entry => ids.contains(entry.getLong(0)))
      .distinct()

    val filtered_edges = edges
      .filter(entry => ids.contains(entry.getLong(0)) && ids.contains(entry.getLong(1)))
      .distinct()

    val nodesRDD = filtered_nodes.mapPartitions(vertices => {
      vertices.map(vertexRow => (vertexRow.getAs[VertexId]("id"), vertexRow.getAs[String]("title")))
    }).rdd

    val edgesRDD = filtered_edges.mapPartitions(edgesRow => {
      edgesRow.map(edgeRow => {
        Edge(edgeRow.getAs[Long]("src"), edgeRow.getAs[Long]("dst"), 1.0)
      })
    }).rdd
    Graph(nodesRDD, edgesRDD)
  }

  def get_subgraphframe(nodes: DataFrame, edges: DataFrame, ids: List[Long]): GraphFrame = {
    nodes
      .filter(entry => ids.contains(entry.getLong(0)))
      .distinct()

    edges
      .filter(entry => ids.contains(entry.getLong(0)) && ids.contains(entry.getLong(1)))
      .distinct()

    GraphFrame(nodes, edges)
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, min: Int, max: Int): Graph[String, Double] = {
    val list = List
      .range(min, max + 1, 1)
      .map(_.toLong)
    get_subgraph(nodes, edges, list)
  }

  def get_subgraphframe(nodes: DataFrame, edges: DataFrame, min: Int, max: Int): GraphFrame = {
    val list = List
      .range(min, max + 1, 1)
      .map(_.toLong)

    get_subgraphframe(nodes, edges, list)
  }

  def get_subgraph(nodes: DataFrame, edges: DataFrame, nr: Int, take_highest: Boolean): Graph[String, Double] = {
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

  def get_subgraphframe(nodes: DataFrame, edges: DataFrame, nr: Int, take_highest: Boolean): GraphFrame = {
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
    get_subgraphframe(nodes, edges, filtered_string_list.map(s => s.toLong))
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

  def most_seen_vertex_sssp_pregel(graph: Graph[String, Double], src_id: Int): Unit = {

    //1. Calculate shortestpath to src given the graph G. sssp will be a collection of all vertices.
    val sssp = shortest_path_pregel(graph, src_id)

    //2. Convert from Array to Map.
    val allVertex = sssp.toMap.flatMap(x => x._2._2)

    //4. (K,V) Calculate how many times we have seen each vertex.
    val dictnodesSeen = allVertex.groupBy(x => x).mapValues(_.size)

    //5. Most seen node(s)
    val mostSeenNode = dictnodesSeen.maxBy(x => x._2)

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
    val subGraphs = ccVertices.groupBy(_._2).mapValues(_.map(_._1))
    //3. Get size of each subGraph and number of subgraphs
    val numberOfSubGraphs = cc.vertices.values.distinct.count
    val subGraphSizes = subGraphs.map(x => (x._1, x._2.size))

  }

  def subgraphs_from_connected_components(graph: Graph[String, Double]): Array[Iterable[VertexId]] = {
    //Calculates connectedComponents for a given graph and returns an array with all the subGraphs.
    println(s"[${Calendar.getInstance().getTime()}] Computing Connected components")
    val cc = graph.connectedComponents()
    println(s"[${Calendar.getInstance().getTime()}] Done computing Connected components")
    val ccVertices = cc.vertices.collect().toMap
    val subGraphs: Array[Iterable[VertexId]] = ccVertices.groupBy(_._2).mapValues(_.keys).values.toArray //Lists with each subgraph

    subGraphs.sortBy(x => x.size)(Ordering[Int].reverse)
  }


  def create_subgraph_from_cc(graph: Graph[String, Double], subGraphItr: Iterable[VertexId]): Graph[String, Double] = {
    //Paramters original graph and subGraph

    //1. Get List of all vertex ids.
    val subGraphVertexIds = subGraphItr.toList

    //2. Get Vertices, filter so that we only take a vertex where Id exist in subgraph.
    val newGraphVertices = graph.vertices.filter {
      case (id, title) => subGraphVertexIds.contains(id)
      case _ => false
    }

    //3. Get edges, filter so that we only take an edge where srcId AND dstId exist in subgraph.
    val newGraphEdges = graph.edges.filter {
      case Edge(srcId, dstId, _) => subGraphVertexIds.contains(srcId) && subGraphVertexIds.contains(dstId)
      case _ => false
    }
    //4. Create subGraph
    val subGraph = Graph(newGraphVertices, newGraphEdges)

    subGraph
  }

  def create_all_subgraphs_from_cc(graph: Graph[String, Double], subGraphs: Array[Iterable[VertexId]], Itr: Int): Array[Graph[String, Double]] = {

    //1. Create array which can hold each subgraph.
    val allGraphs: Array[Graph[String, Double]] = new Array[Graph[String, Double]](Itr)
    //2. Create a loop depending on # of subGraphs
    for (i <- 0 until Itr) {
      allGraphs(i) = create_subgraph_from_cc(graph, subGraphs(i))
    }
    allGraphs
  }


  def filter_from_nodes_using_list(nodes: DataFrame): DataFrame = {
    //Specificy where to save file.
    val filterWords = List("User:", "Help:", "Category talk:", "Template talk:", "Help talk:", "Wikipedia:", "Wikipedia talk:",
      "MediaWiki:", "MediaWiki talk:", "MediaWiki", "Template:", "User talk:", "Talk:", "Module:")

    def containsUdf = udf((strCol: String) => filterWords.exists(strCol.contains))

    val articles = nodes.filter(!containsUdf(col("title")))
    articles

  }

  def filter_from_edges_using_list(nodes: DataFrame, edge: DataFrame): DataFrame = {

    val filterWords = List("User:", "Help:", "Category talk:", "Template talk:", "Help talk:", "Wikipedia:", "Wikipedia talk:",
      "MediaWiki:", "MediaWiki talk:", "MediaWiki", "Template:", "User talk:", "Talk:", "Module:")

    def containsUdf = udf((strCol: String) => filterWords.exists(strCol.contains))

    val users = nodes.filter(containsUdf(col("title")))

    val usersList = users.select("id").collect().map(_ (0)).toList

    val newEdges = edge.filter((!($"src".isin(usersList: _*))) || (!($"dst".isin(usersList: _*))))

    return newEdges
  }

  def compute_filtered_graph() = {
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
/*
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

 */

  def degreeHeurstics(graph: Graph[String, Double]): Graph[Int, Double] = {
    //1. Get outDegree for each vertex (Id,outdegree)
    val g_degOut = graph.outerJoinVertices(graph.outDegrees)((id, title, deg) => deg.getOrElse(0))
    val updatedG = g_degOut.mapTriplets(e => e.dstAttr.toDouble)
    updatedG
  }

  def reProdPath(graph: Graph[(Int, Double), Double], dst: Int): List[Int] = {
    val path = List()
    var currentId = dst
    val seenVertices = graph.vertices.filter(x => (x._2._1 != -1)).collect()
    println(seenVertices.mkString("\n"))

    return path
  }

  def heuristics_shortestPath_pair(graph: Graph[String, Double], src: Int, dst: Int): (Graph[(Int, Double), Double], Int) = {
    //Assuming the graph has the heurstics applied to the edges.
    //Each step over an edge will be a distance +1.

    //1. Calculate outDegree for each node.
    val g = degreeHeurstics(graph)


    val initGraph = g.mapVertices((id, attr) => if (id == src) (src.toInt, 0.0) else (-1, Double.PositiveInfinity))
    //initGraph.vertices.collect().map(vertex => vertex._2._1)
    //Each node has (ID,  (previousID, distance))

    //Return
    val resultingGraph = runShortestPath_deg(initGraph, src, dst, src, 0.0)
    resultingGraph

  }

  def runShortestPath_deg(graph: Graph[(Int, Double), Double], currentId: Int, dst: Int, cameFrom: Int, distance: Double): (Graph[(Int, Double), Double], Int) = {

    //Returns a tuple (Graph, int) if tuple._2 == 1 path was found else not.

    //1. Check if currentId is DST.
    if (currentId == dst) {
      //Update dst vertex.
      return (graph.mapVertices((id, attr) => if (id == dst) (cameFrom, distance) else (attr._1, attr._2)), 1)
    }

    //2. Update current node. Update camefrom and distance.
    val updatedGraph = graph.mapVertices((id, attr) => if (id == currentId && (attr._2 == Double.PositiveInfinity || attr._2 > distance)) (cameFrom, distance) else (attr._1, attr._2))

    //3. Find neighbors for current ID.
    val neighbors = graph.edges.collect().filter(e => (e.srcId == currentId))


    //4. Get top 3 outDeg neighbors.
    val sortNeighbors = neighbors.sortBy(e => e.attr).map(e => e.dstId).toList //Sort byDeg, save only the ID.
    val topNeig = sortNeighbors.take(5)

    //If dst is found in list return. But dst might be found
    if(sortNeighbors.contains(dst)) {
      //Return the last call.
      return(runShortestPath_deg(updatedGraph,dst,dst,currentId,distance+1.0))
    }

    //5. Visit the best neighbors(highest deg)

    for (nextId <- topNeig) {

      //Run recursive
      println("I am on vertex: ",currentId + "My neighbor I am looking at is: ",nextId)

      //check if neighbors distance is greater than current distance. IF yes, update it.
      val neig = updatedGraph.vertices.filter(v => v._1 == nextId).take(1)

      if(neig(0)._2._2 > distance) {
        val result = runShortestPath_deg(updatedGraph, nextId.toInt, dst, currentId, distance + 1.0)

        if (result._2 == 1) {
          return result
        }
      }
    }
    println("No path found sry: ")
    (updatedGraph, 0)
  }

  def heuristic_sssp_pregel(graph: Graph[String,Double], src_id: Int, n:Int): Array[(VertexId,(Double,List[VertexId]))] = {
    //n is how many of highest outDeg neighbours we take.
    //Initiallize the graph.
    val edges = graph.edges.collect()
    val initGraph: Graph[(Double, List[VertexId]), Double] =
      graph.mapVertices((id,_) => if(id==src_id)(0.0,List[VertexId](src_id)) else (Double.PositiveInfinity, List[VertexId]()))


    val sssp = initGraph.pregel((Double.PositiveInfinity, List[VertexId]()),Int.MaxValue, EdgeDirection.Out)(
      (id, attr, msg) => if(msg._1 < attr._1) msg else attr,

      triplet => {

        val neighbours = edges.filter(e => e.srcId == triplet.srcId).sortBy(e => e.attr).map(e => e.dstId).toList.take(n)
        if(neighbours.contains(triplet.dstId) && (triplet.srcAttr._1 < (triplet.dstAttr._1 - triplet.attr))){
          Iterator((triplet.dstId, (triplet.srcAttr._1 + triplet.attr, triplet.srcAttr._2:+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => if(a._1 < b._1) a else b)

    sssp.vertices.collect()

  }

  def heurstics_sssp_pregel1(graph: Graph[String, Double], src_id: Int): Array[(VertexId, (Double, List[VertexId]))] = {
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

    sssp1.vertices.collect()
  }
}
