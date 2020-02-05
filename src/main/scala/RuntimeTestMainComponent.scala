import java.util.Calendar

import Util._

object RuntimeTestMainComponent {
  val filtered_graph = get_filtered_graph()

  def compute_runtime(nodes: List[Double]): Unit = {
    var runtime_graphx = 0.0
    var runtime_pregel = 0.0
    var avg_path_length = 0.0
    val filtered_subgraph = filtered_graph.subgraph(e => nodes.contains(e.srcId) || nodes.contains(e.dstId), (v, _) => nodes.contains(v))
    var size = filtered_subgraph.vertices.collect().length
    val r = scala.util.Random

    println(s"${size / 1000}K:")
    for (i <- 0 until 30) {
      val source_id = filtered_subgraph.vertices.collect()(r.nextInt() % size)._1 //random node
      var start = System.nanoTime()
      shortest_path_graphx(filtered_subgraph, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtime_graphx += runtime.toFloat

      start = System.nanoTime()
      val vertices = shortest_path_pregel(filtered_subgraph, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to pregel")
      runtime_pregel += runtime.toFloat

      vertices.foreach(v => {
        avg_path_length += v._2._2.length / size / 30
      })
      println("Current average path length: " + avg_path_length)
    }
    val avg_runtime_graphx = runtime_graphx / 30
    val avg_runtime_pregel = runtime_pregel / 30
    println(s"${size / 1000}K, graphx: $avg_runtime_graphx ms")
    println(s"${size / 1000}K, pregel: $avg_runtime_pregel ms")
  }

  def main(args: Array[String]): Unit = {
    println(s"[${Calendar.getInstance().getTime}] Computing filtered graph")
    val connected_components = subgraphs_from_connected_components(filtered_graph).sortBy(c => -c.size)
    val biggest_component = connected_components(0).map(v => v.toDouble).toList

    println(s"[${Calendar.getInstance().getTime}] Computing runtime for biggest component")
    compute_runtime(biggest_component)
  }
}
