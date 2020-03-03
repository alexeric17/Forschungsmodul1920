package util

import org.apache.spark.sql.functions._
import util.Util._

object ConvertBWToDF {
  def main(args: Array[String]): Unit = {

    //1. Read data which was created by bluewordsextraction-project - MAKE SURE THAT THE FILE EXISTS
    val bluewordsDF = spark.read.json(FM1920HOME + "/data/bluewords.json")

    //2. Create node dataframe by dropping and renaming some columns
    val nodeDF = bluewordsDF
      .drop("Blue Words")
      .drop("#BW")
      .withColumn("id", bluewordsDF("id").cast("int"))

    //3. Create map which maps titles to their respective ids
    val titleToId = nodeDF
      .rdd
      .map(e => (e.getString(1).toLowerCase, e.getInt(0)))
      .collectAsMap()


    //4. Create Edge Dataframe by translating every blueword to its matching id
    val explodedBluewordsDF = bluewordsDF
      .drop("#BW")
      .drop("title")
      .withColumn("id", bluewordsDF("id").cast("int"))
      .withColumnRenamed("id", "src")
      .withColumn("blueword", explode(bluewordsDF("Blue Words")))
      .drop("Blue Words")

    //UDF to remove the brackets around each blueword before finding a suitable id in the titleToId-map
    val convertBWToIdUdf = udf((s: String) => titleToId.get(s.substring(2, s.length - 2)))

    val edgeDF = explodedBluewordsDF
      .withColumn("id2", convertBWToIdUdf(explodedBluewordsDF("blueword")))
      .drop("blueword")
      .filter("id2 is not null")
      .withColumnRenamed("id2", "dst")

    //5. Save the results to data-directory
    nodeDF.coalesce(1).write.json(nodeDir)
    edgeDF.coalesce(1).write.json(edgeDir)
  }
}
