package runtime_tests

import java.util.Calendar

import util.Util._

object RuntimeTestFilteredGraph {
  val filtered_graph = get_filtered_graph()

  def compute_runtime(): Unit = {
    var runtime_graphx = 0.0
    var runtime_pregel = 0.0
    var size = filtered_graph.vertices.collect().length
    val r = scala.util.Random

    println(s"${size / 1000}K:")
    for (i <- 0 until 30) {
      val source_id = filtered_graph.vertices.collect()(math.abs(r.nextInt() % size))._1 //random node
      println("Computing SSSP for vertex with id " + source_id)

      var start = System.nanoTime()
      shortest_path_graphx(filtered_graph, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtime_graphx += runtime.toFloat

      start = System.nanoTime()
      shortest_path_pregel(filtered_graph, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to pregel")
      runtime_pregel += runtime.toFloat

    }
    val avg_runtime_pregel = runtime_pregel / 30
    val avg_runtime_graphx = runtime_graphx / 30

    println(s"${size / 1000}K, graphx: $avg_runtime_graphx ms")
    println(s"${size / 1000}K, pregel: $avg_runtime_pregel ms")

  }

  def main(args: Array[String]): Unit = {
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for filtered graph")
    compute_runtime()
  }
}
