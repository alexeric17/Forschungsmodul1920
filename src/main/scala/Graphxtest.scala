import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.graphframes.GraphFrame

object Graphxtest {
  def main(args: Array[String]): Unit = {
    //set this to the home directory of this project
    val FM1920HOME = ""

    val conf = new SparkConf().setAppName("Graphxtest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Graphxtest")
      .enableHiveSupport()
      .getOrCreate()

    val nodes = spark.read.json(FM1920HOME + "/data/nodes/nodes.json")

    val edges = spark.read.json(FM1920HOME + "/data/edges/edge.json")

    val graph = GraphFrame(nodes, edges)

    val ranks = graph.pageRank.maxIter(1).run().vertices

    ranks.collect().foreach(n => println(n))
  }
}
