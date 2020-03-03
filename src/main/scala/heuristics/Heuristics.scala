package heuristics

import java.util.Calendar

import org.apache.spark.graphx.{EdgeDirection, Graph, VertexId}
import util.Util._
import util.Util.spark.implicits._

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer //needed to avoid defining implicit encoders when serializing data to rdd

object Heuristics {
  def degreeHeurstics(graph: Graph[String, Double]): Graph[Double, Double] = {
    //1. Get outDegree for each vertex (Id,outdegree)
    val g_degOut = graph.outerJoinVertices(graph.outDegrees)((id, title, deg) => deg.getOrElse(0).toDouble)
    val updatedG = g_degOut.mapTriplets(e => e.dstAttr)
    updatedG
  }

  def pagerankHeuristics(graph: Graph[String, Double]): Graph[Double, Double] = {
    val pageranks = spark.read.json(pagerankFile)
      .as[(VertexId, Double)]
      .rdd

    val g_pagerank: Graph[Double, Double] = graph.outerJoinVertices(pageranks)((id_, title, pagerank) => pagerank.getOrElse(0))
    val updatedG: Graph[Double, Double] = g_pagerank.mapTriplets(e => e.dstAttr)
    updatedG
  }

  def betweennessCentralityHeuristics(graph: Graph[String, Double]): Graph[Double, Double] = {
    val bc_values = spark.read.json(betweennessCentralityFile)
      .as[(VertexId, Double)]
      .rdd

    val g_bc: Graph[Double, Double] = graph.outerJoinVertices(bc_values)((id_, title, bc_value) => bc_value.getOrElse(0))
    val updatedG: Graph[Double, Double] = g_bc.mapTriplets(e => e.dstAttr)
    updatedG
  }

  def heuristics_shortestPath_pair(graph: Graph[String, Double], src: Int, dst: Int, nr_neighbors: Int): (Graph[(Int, Double), Double], Int) = {
    //Assuming the graph has the heurstics applied to the edges.
    //Each step over an edge will be a distance +1.

    //1. Calculate outDegree for each node.
    val g = degreeHeurstics(graph)


    val initGraph = g.mapVertices((id, attr) => if (id == src) (src.toInt, 0.0) else (-1, Double.PositiveInfinity))
    //initGraph.vertices.collect().map(vertex => vertex._2._1)
    //Each node has (ID,  (previousID, distance))

    //Return
    val resultingGraph = runShortestPath_deg(initGraph, src, dst, src, 0.0, nr_neighbors)
    val path = reProdPath(resultingGraph._1, src, dst)
    print(path)
    resultingGraph
  }

  def heuristic_sssp_pregel(graph: Graph[String, Double], src_id: Int, dst_id: Int, n: Int): List[VertexId] = {
    //n is how many of highest outDeg neighbours we take.
    //Initiallize the graph.
    val annotated_graph = degreeHeurstics(graph)
    val edges = annotated_graph.edges.collect()
    val dstNode = annotated_graph.vertices.filter(v => v._1 == dst_id).collect().take(1)
    var shortestPath = ListBuffer[VertexId]()
    val initGraph: Graph[(Double, List[VertexId]), Double] =
      annotated_graph.mapVertices((id, _) => if (id == src_id) (0.0, List[VertexId](src_id)) else (Double.PositiveInfinity, List[VertexId]()))

    val sssp = initGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
      (id, attr, msg) => if (msg._1 < attr._1) msg else attr,

      triplet => {
        if (shortestPath.nonEmpty) {
          Iterator.empty
        } else if (triplet.dstId == dst_id) {
          triplet.srcAttr._2.foreach(v => shortestPath.append(v))
          shortestPath.append(dst_id)
          Iterator.empty
          //Look at top n neighbours, see if any of them has not been visited yet
        } else if (edges.filter(e => e.srcId == triplet.srcId).sortBy(e => -e.attr).map(e => e.dstId).toList.take(n).contains(triplet.dstId) && (triplet.srcAttr._1 < (triplet.dstAttr._1 - 1))) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.srcAttr._2 :+ triplet.dstId)))
        } else {
          Iterator.empty
        }
      },
      (a, b) => if (a._1 < b._1) a else b)

    shortestPath.toList
  }

  def h_sssp_pregel_graph(graph: Graph[String, Double], src_id: Int, dst_id: Int, n: Int, core_nodes: List[Int]): List[VertexId] = {
    //Apply outDeg on Edges
    val annotated_graph = degreeHeurstics(graph)
    val core_paths = spark.read.json(dataDir + "/core_degrees/core_degrees.json")

    //If src == dst
    if (src_id == dst_id) {
      println("SRC_ID == DST_ID")
      return List[VertexId](src_id, dst_id)
    }
    //If src and dst are core nodes.
    if (core_nodes.contains(src_id) && core_nodes.contains(dst_id)) {
      println("SRC_ID and DST_ID ARE CORE NODES")
      val core_paths = spark.read.json(dataDir + "/core_degrees/core_degrees.json")
      val src_core = src_id
      val dst_core = dst_id

      println(s"[${Calendar.getInstance().getTime}] Looking for shortest path between core ids $src_core and $dst_core")

      val core_connection = core_paths
        .toDF()
        .select("path")
        .where(s"src=$src_core and dst=$dst_core")
        .rdd

      val core_connection_list = core_connection
        .first()
        .getList[Long](0)
        .toList

      return core_connection_list
    }

    val edges = graph.edges.collect()
    val src2core = ListBuffer[VertexId]()

    //Init search from SRC to core and dst.
    val initGraph: Graph[(Double, List[VertexId]), Double] =
      annotated_graph.mapVertices((id, _) => if (id == src_id) (0.0, List[VertexId](src_id)) else (Double.PositiveInfinity, List[VertexId]()))

    val srcSSSP = initGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
      (id, attr, msg) => if (msg._1 < attr._1) msg else attr,

      triplet => {
        //Find edge for triplet SRC node. Sort it by degree.
        val edgesAtNode = edges.filter(e => e.srcId == triplet.srcId).sortBy(e => -e.attr).map(e => e.dstId).toList

        //If DST id is neig to the node. Return path
        if (triplet.dstId == dst_id) {
          val path = triplet.srcAttr._2
          println("PATH FOUND WHEN TRAVERSING from SRC.")
          path.foreach(v => src2core.append(v))
          src2core.append(dst_id)
          Iterator.empty
        }
        //If neigh is a core node.
        else if (core_nodes.contains(triplet.dstId) && src2core.nonEmpty) {
          triplet.srcAttr._2.foreach(v => src2core.append(v))
          src2core.append(triplet.dstId)
          Iterator.empty
        }
        //Take n neighbours with highest Deg. Activate and continue.
        else if (edgesAtNode.take(n).contains(triplet.dstId) && (triplet.srcAttr._1 < (triplet.dstAttr._1 - 1)) && src2core.nonEmpty) {
          Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.srcAttr._2 :+ triplet.dstId)))
        }
        else {
          Iterator.empty
        }
      },
      (a, b) => if (a._1 < b._1) a else b)

    src2core.toList
  }

  def heuristic_sssp_pregel(graph: Graph[String, Double], src_id: Int, dst_id: Int, n: Int, core_nodes: List[Int]): List[VertexId] = {
    //n is how many of highest outDeg neighbours we take.
    //Initiallize the graph.
    val annotated_graph = degreeHeurstics(graph)
    val edges1 = annotated_graph.edges.collect()
    val dstNode = annotated_graph.vertices.filter(v => v._1 == dst_id).collect().take(1)

    var shortestPath = ListBuffer[VertexId]()
    var src2core = ListBuffer[VertexId]()
    var dst2core = ListBuffer[VertexId]()
    if (src_id == dst_id) {
      return List[VertexId](src_id)
    }
    if (edges1.filter(e => e.srcId == src_id).map(e => e.dstId).contains(dst_id)) {
      shortestPath.append(src_id)
      shortestPath.append(dst_id)
      return shortestPath.toList
    }

    val initGraph: Graph[(Double, List[VertexId]), Double] =
      annotated_graph.mapVertices((id, _) => if (id == src_id) (0.0, List[VertexId](src_id)) else (Double.PositiveInfinity, List[VertexId]()))

    println(s"[${Calendar.getInstance().getTime}] Starting Computation of heurstic shortest path from $src_id to $dst_id")

    if (!core_nodes.contains(src_id)) {
      println(s"[${Calendar.getInstance().getTime}] Computing first half of heurstics")
      edges1.filter(e => e.srcId == src_id).foreach(e => println(s"(${e.srcId}, ${e.dstId}): ${e.attr}"))
      val sssp = initGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
        (id, attr, msg) => if (msg._1 < attr._1) msg else attr,

        triplet => {
          val edi = edges1.filter(e => e.srcId == triplet.srcId).sortBy(e => -e.attr).map(e => e.dstId).toList
          //Shortest Path or Core Node found
          if (shortestPath.nonEmpty || src2core.nonEmpty) {
            Iterator.empty
            //Destination in my neighborhood
          }
          else if (triplet.dstId == dst_id) {
            triplet.srcAttr._2.foreach(v => shortestPath.append(v))
            shortestPath.append(dst_id)
            Iterator.empty
            //Core Node in my neighborhood
          } else if (core_nodes.contains(triplet.dstId)) {
            triplet.srcAttr._2.foreach(v => src2core += v)
            src2core += triplet.dstId
            Iterator.empty
            //Look at top n neighbours, see if any of them has not been visited yet
          } else if (edi.take(n).contains(triplet.dstId) && (triplet.srcAttr._1 < (triplet.dstAttr._1 - 1))) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.srcAttr._2 :+ triplet.dstId)))
          } else {
            Iterator.empty
          }
        },
        (a, b) => if (a._1 < b._1) a else b)


    } else {
      println(s"[${Calendar.getInstance().getTime}] Skipped first half of heurstics - $src_id is already in the core")
      src2core += src_id
    }

    if (shortestPath.nonEmpty) {
      println("SP found in first half. Returning: ", shortestPath)
      return shortestPath.toList
    }
    println("src2core length is :", src2core.length, "src2core content is :", src2core.length)

    if (!core_nodes.contains(dst_id)) {

      val reversed_g = graph.reverse
      val rev_g_outDeg = reversed_g.outerJoinVertices(reversed_g.inDegrees)((id, title, deg) => deg.getOrElse(0))
      val reversed_graph = rev_g_outDeg.mapTriplets(e => e.dstAttr.toDouble)
      val edges_rev = reversed_graph.edges.collect()

      val initReversedGraph: Graph[(Double, List[VertexId]), Double] =
        reversed_graph.mapVertices((id, _) => if (id == dst_id) (0.0, List[VertexId](dst_id)) else (Double.PositiveInfinity, List[VertexId]()))

      edges_rev.filter(e => e.srcId == dst_id).foreach(e => println(s"(${e.srcId}, ${e.dstId}): ${e.attr}"))
      println(s"[${Calendar.getInstance().getTime}] Computing second half of heurstics")
      val sssp_reversed = initReversedGraph.pregel((Double.PositiveInfinity, List[VertexId]()), Int.MaxValue, EdgeDirection.Out)(
        (id, attr, msg) => if (msg._1 < attr._1) msg else attr,
        triplet => {

          val edi2 = edges_rev.filter(e => e.srcId == triplet.srcId).sortBy(e => -e.attr).map(e => e.dstId).toList
          if (dst2core.nonEmpty) {
            //Core Node found
            Iterator.empty
          } else if (core_nodes.contains(triplet.dstId)) {
            triplet.srcAttr._2.foreach(v => dst2core += v)
            dst2core += triplet.dstId
            Iterator.empty
            //Look at top n neighbours, see if any of them has not been visited yet
          } else if (triplet.dstId.toInt == src_id) {
            triplet.srcAttr._2.foreach(v => shortestPath.append(v))
            shortestPath.append(src_id)
            shortestPath.reverse
            Iterator.empty
          }
          else if (edi2.take(n).contains(triplet.dstId) && (triplet.srcAttr._1 < (triplet.dstAttr._1 - 1))) {
            Iterator((triplet.dstId, (triplet.srcAttr._1 + 1, triplet.srcAttr._2 :+ triplet.dstId)))
          } else {
            Iterator.empty
          }
        },
        (a, b) => if (a._1 < b._1) a else b)
    } else {
      println(s"[${Calendar.getInstance().getTime}] Skipped second half of heurstics - $dst_id is already in the core")
      dst2core += dst_id
    }

    if (shortestPath.nonEmpty) {
      return shortestPath.toList
    }

    if (src2core.isEmpty || dst2core.isEmpty) {
      //No heuristic path found
      return List()
    }
    //Look in precomputed paths for core node pair and return heuristic path

    val core_paths = spark.read.json(dataDir + "/core_degrees/core_degrees.json")
    val src_core = src2core.last
    val dst_core = dst2core.last

    println(s"[${Calendar.getInstance().getTime}] Looking for shortest path between core ids $src_core and $dst_core")

    val core_connection = core_paths
      .toDF()
      .select("path")
      .where(s"src=$src_core and dst=$dst_core")
      .rdd

    val core_connection_list = core_connection
      .first()
      .getList[Long](0)
      .toList

    var result = ListBuffer[Long]()
    result = result ++ src2core
    core_connection_list.foreach(v => result += v)
    result ++ dst2core.reverse

    result.toList.distinct
  }

  def run_h_sssp_pregel_graph(graph: Graph[String, Double], src_id: Int, dst_id: Int, n: Int, core_nodes: List[Int]): List[VertexId] = {

    val runSrc = h_sssp_pregel_graph(graph, src_id, dst_id, n, core_nodes)
    if (runSrc.contains(src_id) && runSrc.contains(dst_id)) {
      println("Src and dst found! :", runSrc)
      return runSrc
    }

    //Last element should be a core node. Else we didnt find core.
    if (core_nodes.contains(runSrc.last)) {
      val rev_graph = graph.reverse
      val runDst = h_sssp_pregel_graph(rev_graph, dst_id, src_id, n, core_nodes)
      if (core_nodes.contains(runDst.last)) {
        //Cores is in both lists.

        val core_paths = spark.read.json(dataDir + "/core_degrees/core_degrees.json")

        val src_core = runSrc.last
        val dst_core = runDst.last

        println(s"[${Calendar.getInstance().getTime}] Looking for shortest path between core ids $src_core and $dst_core")

        val core_connection = core_paths
          .toDF()
          .select("path")
          .where(s"src=$src_core and dst=$dst_core")
          .rdd

        val core_connection_list = core_connection
          .first()
          .getList[Long](0)
          .toList

        var result = ListBuffer[Long]()
        result = result ++ runSrc
        core_connection_list.foreach(v => result += v)
        result ++ runDst.reverse

        val result_list: List[Long] = result.toList.distinct

        return result_list
      }

    }
    List[VertexId](-1)
  }

  def runShortestPath_deg(graph: Graph[(Int, Double), Double], currentId: Int, dst: Int, cameFrom: Int, distance: Double, nr_neighbors: Int): (Graph[(Int, Double), Double], Int) = {

    //Returns a tuple (Graph, int) if tuple._2 == 1 path was found else not.

    //1. Check if currentId is DST.
    if (currentId == dst) {
      //Update dst vertex.
      return (graph.mapVertices((id, attr) => if (id == dst) (cameFrom, distance) else (attr._1, attr._2)), 1)
    }

    //2. Update current node. Remember where we came from and increase distance.
    val updatedGraph = graph.mapVertices((id, attr) => if (id == currentId && (attr._2 == Double.PositiveInfinity || attr._2 > distance)) (cameFrom, distance) else (attr._1, attr._2))

    //3. Find neighbors for current ID.
    val neighbors = graph.edges.collect().filter(e => e.srcId == currentId)


    //4. Get top 3 outDeg neighbors.
    val sortNeighbors = neighbors.sortBy(e => e.attr).map(e => e.dstId).toList //Sort byDeg, save only the ID.
    val topNeig = sortNeighbors.take(5)

    //If dst is found in list return. But dst might be found
    if (sortNeighbors.contains(dst)) {
      //Return the last call.
      return runShortestPath_deg(updatedGraph, dst, dst, currentId, distance + 1.0, nr_neighbors)
    }

    //4. Get top 3 outDeg neighbors.
    val sortNeighborsByDeg = neighbors.sortBy(e => e.attr).map(e => e.dstId).toList.take(nr_neighbors) //Sort byDeg, save only the ID.


    //5. Visit the best neighbors(highest deg)

    for (nextId <- topNeig) {

      //Run recursive
      println("(" + currentId + "," + nextId + ")")

      //check if neighbors distance is greater than current distance. IF yes, update it.
      val neig = updatedGraph.vertices.filter(v => v._1 == nextId).take(1)

      if (neig(0)._2._2 > distance) {
        val result = runShortestPath_deg(updatedGraph, nextId.toInt, dst, currentId, distance + 1.0, nr_neighbors)

        if (result._2 == 1) {
          return result
        }
      }
    }
    (updatedGraph, 0)
  }

  def heuristics_sssp(annotated_graph: Graph[Double, Double], src_id: Int, dst_id: Int, n: Int, path_to_core_dataframe: String): List[VertexId] = {
    val edges = annotated_graph.edges.collect()
    var src2core = ListBuffer[VertexId]()
    var dst2core = ListBuffer[VertexId]()
    val queue = mutable.Queue[(Long, ListBuffer[Long])]()
    val visited_nodes = ListBuffer[VertexId]()
    var result = ListBuffer[Long]()

    if (src_id == dst_id) {
      return List[VertexId](src_id)
    }

    val core = spark.read.json(path_to_core_dataframe).toDF()
    val core_nodes = core.select("src").distinct().collect().toList.map(r => r.getLong(0).toInt)

    if (core_nodes.contains(src_id)) {
      src2core += src_id
    } else {

      queue.enqueue((src_id, ListBuffer()))

      while (queue.nonEmpty && src2core.isEmpty) {
        val current = queue.dequeue()
        val current_id = current._1
        val current_path = current._2
        current_path += current_id

        visited_nodes += current_id

        val current_neighborhood = edges
          .distinct
          .filter(e => (e.srcId == current_id) && (!visited_nodes.contains(e.dstId))).sortBy(-_.attr)

        //Check if destination is in current neighborhood
        current_neighborhood.foreach(e => if (e.dstId == dst_id) {
          //Destination in my neighborhood
          current_path.foreach(v => result += v)
          result += dst_id
          return result.toList
        })

        //Check if any core node is in current neighborhood
        val neighbored_cores = current_neighborhood
          .filter(e => core_nodes.contains(e.dstId))

        if (neighbored_cores.nonEmpty) {
          current_path.foreach(v => src2core += v)
          src2core += neighbored_cores(0).dstId
        } else {
          //Nothing found here - add next nodes
          current_neighborhood
            .filter(e => !queue.toMap.contains(e.dstId))
            .take(n)
            .foreach(e => queue.enqueue((e.dstId, current_path)))
        }
      }
    }

    if (src2core.isEmpty) {
      return List()
    }

    if (core_nodes.contains(dst_id)) {
      dst2core += dst_id
    } else {
      val reversed_g = annotated_graph.reverse
      val rev_g_inDeg = reversed_g.outerJoinVertices(reversed_g.inDegrees)((id, title, deg) => deg.getOrElse(0))
      val reversed_graph = rev_g_inDeg.mapTriplets(e => e.dstAttr.toDouble)
      val edges_rev = reversed_graph.edges.collect()
      queue.clear()
      visited_nodes.clear()

      queue.enqueue((dst_id, ListBuffer()))

      while (queue.nonEmpty && dst2core.isEmpty) {
        val current = queue.dequeue()
        val current_id = current._1
        val current_path = current._2
        current_path += current_id
        visited_nodes += current_id

        val current_neighborhood = edges_rev
          .distinct
          .filter(e => e.srcId == current_id && (!visited_nodes.contains(e.dstId))).sortBy(-_.attr)

        //Check if any node in current neighborhood is part of src2core
        val neighbored_path_nodes = current_neighborhood
          .filter(e => src2core.contains(e.dstId))

        if (neighbored_path_nodes.nonEmpty) {
          //Found connection to src2core path - end computation and return found path
          val found_node = neighbored_path_nodes(0).dstId
          while (src2core.head != found_node) {
            result += src2core.head
            src2core.remove(0)
          }
          result += found_node
          result ++= current_path.reverse
          return result.toList
        }

        //Check if any core node is in current neighborhood
        val neighbored_cores = current_neighborhood.filter(e => core_nodes.contains(e.dstId))
        if (neighbored_cores.nonEmpty) {
          current_path.foreach(v => dst2core += v)
          dst2core += neighbored_cores(0).dstId
        } else {
          //Nothing found here - add next nodes
          current_neighborhood
            .filter(e => !queue.toMap.contains(e.dstId))
            .take(n)
            .foreach(e => queue.enqueue((e.dstId, current_path)))
        }
      }
    }

    if (dst2core.isEmpty) {
      return List()
    }

    val core_paths = spark.read.json(path_to_core_dataframe)
    val src_core = src2core.last
    val dst_core = dst2core.last

    val core_connection = core_paths
      .toDF()
      .select("path")
      .where(s"src=$src_core and dst=$dst_core")
      .rdd

    if (core_connection.collect().isEmpty) {
      //Unfortunately, found two core nodes which have no connection :(
      return List()
    }

    val core_connection_list = core_connection
      .first()
      .getList[Long](0)
      .toList

    result ++= src2core
    result ++= core_connection_list.subList(1, core_connection_list.length - 1)
    result ++= dst2core.reverse

    result.toList
  }
}
