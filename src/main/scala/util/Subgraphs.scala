package util

import java.util.Calendar

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, desc}
import org.graphframes.GraphFrame
import util.Util._
import util.Util.spark.implicits._

object Subgraphs {

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

  def subgraphs_from_connected_components(graph: Graph[String, Double]): Array[Iterable[VertexId]] = {
    //Calculates connectedComponents for a given graph and returns an array with all the subGraphs.
    println(s"[${Calendar.getInstance().getTime}] Computing Connected components")
    val cc = graph.connectedComponents()
    println(s"[${Calendar.getInstance().getTime}] Done computing Connected components")
    val ccVertices = cc.vertices.collect().toMap
    val subGraphs: Array[Iterable[VertexId]] = ccVertices.groupBy(_._2).mapValues(_.keys).values.toArray //Lists with each subgraph

    subGraphs.sortBy(x => x.size)(Ordering[Int].reverse)
  }


  def create_subgraph_from_cc(graph: Graph[String, Double], subGraphItr: Iterable[VertexId]): Graph[String, Double] = {
    println(s"[${Calendar.getInstance().getTime}] Called create_subgraph_from_cc for subgraph of size ${subGraphItr.size}")
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
}
