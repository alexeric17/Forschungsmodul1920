package RuntimeTests

import java.util.Calendar

import util.Subgraphs._
import util.Util._

import scala.collection.mutable.ListBuffer

object RuntimeTest {
  val filtered_graph = get_filtered_graph()

  def compute_runtime(nodes: List[Double]): Unit = {
    var runtimes_graphx = new ListBuffer[Double]()
    var runtimes_pregel = new ListBuffer[Double]()
    val filtered_subgraph = filtered_graph.subgraph(e => nodes.contains(e.srcId) || nodes.contains(e.dstId), (v, _) => nodes.contains(v))
    var size = filtered_subgraph.vertices.collect().length
    val source_id = filtered_subgraph.vertices.collect()(0)._1

    println(s"${size / 1000}K:")
    for (i <- 0 until 10) {
      var start = System.nanoTime()
      shortest_path_graphx(filtered_subgraph, List(source_id), source_id.toInt)
      var runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to graphx")
      runtimes_graphx += runtime

      start = System.nanoTime()
      shortest_path_pregel(filtered_subgraph, source_id.toInt)
      runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to pregel")
      runtimes_pregel += runtime
    }
    val avg_runtime_graphx = runtimes_graphx.sum.toLong / runtimes_graphx.length
    val avg_runtime_pregel = runtimes_pregel.sum.toLong / runtimes_pregel.length
    val stddev_graphx = math.sqrt(runtimes_graphx.map(a => math.pow(a - avg_runtime_graphx, 2)).sum / runtimes_graphx.size)
    val stddev_pregel = math.sqrt(runtimes_pregel.map(a => math.pow(a - avg_runtime_pregel, 2)).sum / runtimes_pregel.size)
    val stderr_graphx = stddev_graphx / math.sqrt(runtimes_graphx.length)
    val stderr_pregel = stddev_pregel / math.sqrt(runtimes_pregel.length)
    println(s"${size / 1000}K, graphx: $avg_runtime_graphx ms [standard error: $stderr_graphx]")
    println(s"${size / 1000}K, pregel: $avg_runtime_pregel ms [standard error: $stderr_pregel]")
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
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 50000")
    compute_runtime(biggest_component.take(50000))
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for size 150000")
    compute_runtime(biggest_component.take(150000))
  }
}
