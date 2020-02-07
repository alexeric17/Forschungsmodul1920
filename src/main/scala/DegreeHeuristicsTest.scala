import Util._

object DegreeHeuristicsTest {
  def main(args: Array[String]): Unit = {
    val filteredGraph = get_filtered_graph()
    val src_id = 9968
    filteredGraph.vertices.collect().foreach(v => heuristics_shortestPath_pair(filteredGraph, src_id, v._1.toInt, 3))
  }
}
