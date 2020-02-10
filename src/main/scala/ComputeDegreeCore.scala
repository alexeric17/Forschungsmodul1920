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

    println(s"[${Calendar.getInstance().getTime}] Computing In Degrees for Degree Heuristics Core")

    val g_degIn = filtered_graph.outerJoinVertices(filtered_graph.inDegrees)((_, _, deg) => deg.getOrElse(0))
      .vertices.collectAsMap()

    println(s"[${Calendar.getInstance().getTime}] Computing Out Degrees for Degree Heuristics Core")

    val g_degOut = filtered_graph.outerJoinVertices(filtered_graph.outDegrees)((_, _, deg) => deg.getOrElse(0))
      .vertices

    val core_nodes = g_degOut
      .filter(v => g_degIn(v._1) != 0)
      .sortBy(v => -v._2).take(100)

    println(s"Computing the core on graph of size ${filtered_graph.vertices.collect().length} for the following vertices:")
    core_nodes.foreach(v => println(s"ID ${v._1} with degree ${v._2} (In-degree: ${g_degIn(v._1)})"))

    core_nodes.foreach(dst => {
      val start = System.nanoTime()
      val paths = shortest_path_pregel(filtered_graph, dst._1.toInt)
      println(s"Done computing after ${(System.nanoTime() - start) / 1000 / 1000} ms")
      paths.foreach(src => {
        nr_total_paths += 1
        if (src._2._2.nonEmpty) {
          nr_found_paths += 1
        }
        result((src._1, dst._1)) = src._2._2
      })
    })
    println(s"Percentage of found paths: ${nr_found_paths.toDouble / nr_total_paths}")
    result.toSeq.toDF("pair", "shortest_path").coalesce(1).write.json(dataDir + "/core_degree")
  }
}
