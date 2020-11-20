package streaming

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import collection.JavaConverters._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.{ FileSystem, FileUtil, Path }
import org.apache.hadoop.conf.Configuration

object NetworkWordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      System.exit(1)
    }

    //StreamingExamples.setStreamingLogLevels()
    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local")
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(20))
 
    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    val lines = ssc.textFileStream(args(0))
    val words = lines.flatMap(_.split(" "))
    val toRemove = List('\n','\t', '-', ',', ';', '[',']','{','}','(',')','<','>',' ','.',':','?','!','\"','\'')

    // Task 1
    val wordCounts = words.map(x => (x.filterNot(toRemove.contains(_)), 1)).reduceByKey(_ + _)
    // Task 2
    val shortWordCounts = wordCounts.filter(x => x._1.length >=5)
    // Task 3
    val matrix = lines
      .map(_.split(" ").map(_.trim).toList)
      .flatMap{ strr => // making item pairs
      var itemPairsWithFreq = ArrayBuffer[((String, String), Int)]()
      for ((item1) <- strr) {
        for ((item2) <- strr) {
          val pairWithFreq = ((item1.filterNot(toRemove.contains(_)), item2.filterNot(toRemove.contains(_))), 1)
          itemPairsWithFreq += pairWithFreq
        }
      }
      itemPairsWithFreq
    }.reduceByKey(_ + _)



    def saveAsTextFileAndMerge[T](hdfsServer: String, fileName: String, rdd: RDD[T]) = {
      val sourceFile = hdfsServer + "/tmp/"
      rdd.saveAsTextFile(sourceFile)
      val dstPath = hdfsServer + "/final/"
      merge(sourceFile, dstPath, fileName)
    }

    def merge(srcPath: String, dstPath: String, fileName: String): Unit = {
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      val destinationPath = new Path(dstPath)
      if (!hdfs.exists(destinationPath)) {
        hdfs.mkdirs(destinationPath)
      }
      FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath + "/" + fileName), false, hadoopConfig, null)
    }

    //Output of Task 1
    wordCounts.print()
    wordCounts.foreachRDD(rdd => rdd.saveAsTextFile(args(1)))


    //Output of Task 2
    shortWordCounts.print()
    shortWordCounts.foreachRDD(rdd => rdd.saveAsTextFile(args(2)))

    //Output of Task 3
    matrix.print()
    matrix.foreachRDD(rdd => rdd.saveAsTextFile(args(3)))

    ssc.start()
    ssc.awaitTermination()
  }
}