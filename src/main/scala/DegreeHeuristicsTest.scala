import Util._

object DegreeHeuristicsTest {
  def main(args: Array[String]): Unit = {
    val filteredGraph = get_filtered_graph()
    val src_id = 1
    filteredGraph.vertices.collect().foreach(v => {
      println(reProdPath(heuristics_shortestPath_pair(filteredGraph, src_id, v._1.toInt, 3)._1, src_id, v._1.toInt).toString())
    })
  }
}
