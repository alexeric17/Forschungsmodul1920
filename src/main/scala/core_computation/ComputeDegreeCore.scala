package core_computation

import java.util.Calendar

import org.apache.spark.graphx.VertexId
import util.Subgraphs._
import util.Util._
import util.Util.spark.implicits._

import scala.collection.mutable.ListBuffer

object ComputeDegreeCore {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()
    var result = ListBuffer[(VertexId, VertexId, List[VertexId])]()

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
      .sortBy(v => -v._2).take(1000)

    println(s"Computing the core on graph of size ${filtered_graph.vertices.collect().length} for the following vertices:")
    core_nodes.foreach(v => println(s"ID ${v._1} with degree ${v._2})"))

    val core_node_ids = core_nodes.map(n => n._1)

    var iteration = 0
    core_node_ids.foreach(dst => {
      iteration += 1
      val start = System.nanoTime()
      val paths = shortest_path_pregel(filtered_graph, dst.toInt)
      println(s"Done computing after ${(System.nanoTime() - start) / 1000 / 1000} ms")
      paths
        .filter(v => core_node_ids.contains(v._1) && v._2._2.nonEmpty)
        .foreach(v => result.append((v._2._2.head, v._2._2.last, v._2._2)))

      if (iteration == 100 || (iteration > 100 && iteration % 10 == 0)) {
        println(s"Percentage of found paths between core after $iteration iterations: ${result.length.toDouble / 10000}")
        result.filter(t => core_node_ids.take(iteration).contains(t._1) && core_node_ids.take(iteration).contains(t._2))
          .toDF("src", "dst", "path").coalesce(1).write.json(dataDir + s"/core_degrees/$iteration")
      }
    })
  }
}
