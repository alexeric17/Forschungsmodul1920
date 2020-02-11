import java.util.Calendar

import Util._

import scala.collection.mutable

object RuntimeTestFilteredGraph {
  val filtered_graph = get_filtered_graph()

  def compute_runtime(): Unit = {
    var runtime_pregel = 0.0
    var pathlengths = new mutable.HashMap[Int, Int]
    var size = filtered_graph.vertices.collect().length
    val r = scala.util.Random

    println(s"${size / 1000}K:")
    for (i <- 0 until 30) {
      val source_id = filtered_graph.vertices.collect()(math.abs(r.nextInt() % size))._1 //random node
      println("Computing SSSP for vertex with id " + source_id)
      val start = System.nanoTime()
      val vertices = shortest_path_pregel(filtered_graph, source_id.toInt)
      val runtime = (System.nanoTime() - start) / 1000 / 1000
      println("Adding " + runtime + " ms to pregel")
      runtime_pregel += runtime.toFloat

      vertices.foreach(v => {
        val length = v._2._2.length
        if (!pathlengths.contains(length)) {
          pathlengths(length) = 1
        } else {
          pathlengths(length) += 1
        }
      })
      println("Current average path lengths: ")
      pathlengths.foreach(p => println("Nr of paths of Length " + p._1 + ": " + p._2))
    }
    val avg_runtime_pregel = runtime_pregel / 30
    println(s"${size / 1000}K, pregel: $avg_runtime_pregel ms")
    pathlengths.foreach(p => println("Nr of paths of Length " + p._1 + ": " + p._2))

  }

  def main(args: Array[String]): Unit = {
    println(s"[${Calendar.getInstance().getTime}] Computing runtime for filtered graph")
    compute_runtime()
  }
}
