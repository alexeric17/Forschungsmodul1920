import Util._

object Graphxtest {
  def main(args: Array[String]): Unit = {

    val test = heuristic_sssp_pregel(get_filtered_graph(), 1, 141728, 3, get_core_node_ids())

    println(test.toString())
  }
}
