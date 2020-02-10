import java.util.Calendar

import Util._
import org.apache.spark.graphx.lib.ShortestPaths

object ComputeDegreeCore {
  def main(args: Array[String]): Unit = {
    val filtered_graph = get_filtered_graph()

    println(s"[${Calendar.getInstance().getTime}] Computing In Degrees for Degree Heuristics Core")

    val g_degIn = filtered_graph.outerJoinVertices(filtered_graph.inDegrees)((_, _, deg) => deg.getOrElse(0))
      .vertices.collectAsMap()

    println(s"[${Calendar.getInstance().getTime}] Computing Out Degrees for Degree Heuristics Core")

    val g_degOut = filtered_graph.outerJoinVertices(filtered_graph.outDegrees)((_, _, deg) => deg.getOrElse(0))
      .vertices

    val core_nodes = g_degOut
      .filter(v => g_degIn(v._1) != 0)
      .sortBy(v => -v._2).take(100)

    println("Computing the core for the following vertices:")
    core_nodes.foreach(v => println(s"ID ${v._1} with degree ${v._2} (In-degree: ${g_degIn(v._1)}"))

    val start = System.nanoTime()
    val result = ShortestPaths.run(filtered_graph, core_nodes.map(v => v._1))
    println(s"Done computing after ${(System.nanoTime() - start) / 1000 / 1000} ms")

    spark.createDataFrame(result.vertices.distinct()).toDF().coalesce(1).write.json(FM1920HOME + "/data/core_degree")
  }
}
