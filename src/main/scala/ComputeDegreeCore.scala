import java.util.Calendar

import Util._
import org.apache.spark.graphx.VertexId

import scala.collection.mutable
import spark.implicits._

object ComputeDegreeCore {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    val result = mutable.Map[(VertexId, VertexId), List[VertexId]]()
    var nr_found_paths = 0
    var nr_total_paths = 0

    val connected_components = subgraphs_from_connected_components(filtered_graph).sortBy(c => -c.size)
    val biggest_component = connected_components(0).toSet

    val g_degIn = filtered_graph.outerJoinVertices(filtered_graph.inDegrees)((_, _, deg) => deg.getOrElse(0))
      .vertices.collect().toMap

    println(s"[${Calendar.getInstance().getTime}] Computing Out Degrees for Degree Heuristics Core")

    val g_degOut = filtered_graph.outerJoinVertices(filtered_graph.outDegrees)((_, _, deg) => deg.getOrElse(0))
      .vertices

    val core_nodes = g_degOut
      .filter(v => biggest_component.contains(v._1))
      .filter(v => g_degIn(v._1) > 0)
      .sortBy(v => -v._2).take(100)

    println(s"Computing the core on graph of size ${filtered_graph.vertices.collect().length} for the following vertices:")
    core_nodes.foreach(v => println(s"ID ${v._1} with degree ${v._2})"))

    val core_node_ids = core_nodes.map(n => n._1)

    core_node_ids.foreach(dst => {
      val start = System.nanoTime()
      val paths = shortest_path_pregel(filtered_graph, dst.toInt)
      println(s"Done computing after ${(System.nanoTime() - start) / 1000 / 1000} ms")
      paths.foreach(src => {
        nr_total_paths += 1
        if (src._2._2.nonEmpty) {
          nr_found_paths += 1
        }
        result((src._1, dst)) = src._2._2
      })
    })
    println(s"Percentage of found paths: ${nr_found_paths.toDouble / nr_total_paths}")
    val core_edges = result.filter(v => core_node_ids.contains(v._1._1) && core_node_ids.contains(v._1._2))
    println(s"Percentage of found paths between core: ${core_edges.toArray.length.toDouble / 10000}")
    result.toSeq.toDF("pair", "shortest_path").coalesce(1).write.json(dataDir + "/core_degree")
  }
}
