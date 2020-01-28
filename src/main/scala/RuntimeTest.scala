import java.util.Calendar

import Util._

object RuntimeTest {
  val filtered_graph = get_filtered_graph()

  def compute_runtime(nodes: List[Double]): Unit = {
    var runtime_graphx = 0.0
    var runtime_pregel = 0.0
    val filtered_subgraph = filtered_graph.subgraph(e => nodes.contains(e.srcId) || nodes.contains(e.dstId), (v, _) => nodes.contains(v))
    var size = filtered_subgraph.vertices.collect().length
    val source_id = filtered_subgraph.vertices.collect()(0)._1

    println(s"${size / 1000}K:")
    for (i <- 0 until 10) {
      var start = System.nanoTime()
      shortest_path_graphx(filtered_subgraph, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtime_graphx += runtime.toFloat

      start = System.nanoTime()
      shortest_path_pregel(filtered_subgraph, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to pregel")
      runtime_pregel += runtime.toFloat
    }
    val avg_runtime_graphx = runtime_graphx / 10
    val avg_runtime_pregel = runtime_pregel / 10
    println(s"${size / 1000}K, graphx: $avg_runtime_graphx ms")
    println(s"${size / 1000}K, pregel: $avg_runtime_pregel ms")
  }

  def main(args: Array[String]): Unit = {
    println(s"[${Calendar.getInstance().getTime}] Computing filtered graph")
    val connected_components = subgraphs_from_connected_components(filtered_graph).sortBy(c => -c.size)
    val biggest_component = connected_components(0).map(v => v.toDouble).toList

    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 5000")
    compute_runtime(biggest_component.take(5000))
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 10000")
    compute_runtime(biggest_component.take(10000))
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 25000")
    compute_runtime(biggest_component.take(25000))
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 5ß000")
    compute_runtime(biggest_component.take(50000))
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 150000")
    compute_runtime(biggest_component.take(150000))
  }
}
