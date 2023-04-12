package dpla.entries

import dpla.executors.ThumbnailPurge
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ThumbnailPurgeEntry {

  def main(args: Array[String]): Unit = {

    val inpath = args(0)

    val conf: SparkConf = new SparkConf()
      .setAppName("Batch process: Purge Thumbnails")
      .setMaster("local[*]")

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()

    ThumbnailPurge.execute(spark, inpath)

    spark.stop()
  }
}
