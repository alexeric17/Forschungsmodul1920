import org.apache.spark.graphx.Edge
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql._
import org.graphframes.GraphFrame

object Graphxtest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Graphxtest").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val spark = SparkSession
      .builder()
      .appName("Graphxtest")
      .enableHiveSupport()
      .getOrCreate()

    val nodes = spark.createDataFrame(sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
      (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))).toDF("id", "information")

    val edges = spark.createDataFrame(sc.parallelize(Array(Edge(3L, 7L, "collab"), Edge(5L, 3L, "advisor"),
      Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))).toDF("src", "dst", "information")

    val graph = GraphFrame(nodes, edges)

    val ranks = graph.pageRank.maxIter(1).run().vertices

    ranks.collect().foreach(n => println(n))
  }
}
