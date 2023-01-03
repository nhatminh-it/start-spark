
import org.apache.log4j.{Level, Logger}
import org.apache.spark
import org.apache.spark.ml.feature.QuantileDiscretizer
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partition, SparkConf, SparkContext, TaskContext}
import org.apache.spark.sql.functions.{aggregate, column, to_timestamp, unix_timestamp}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.IOUtils

import java.io.IOException
import java.text.SimpleDateFormat
import java.util.Calendar
import java.time.{LocalDateTime, ZoneId}


object Main {

  import scala.reflect.ClassTag


  // Each random partition will hold `numValues` items
  final class RandomPartition[A: ClassTag](val index: Int, numValues: Int, random: => A) extends Partition {
    def values: Iterator[A] = Iterator.fill(numValues)(random)
  }

  // The RDD will parallelize the workload across `numSlices`
  final class RandomRDD[A: ClassTag](@transient private val sc: SparkContext, numSlices: Int, numValues: Int, random: => A) extends RDD[A](sc, deps = Seq.empty) {

    // Based on the item and executor count, determine how many values are
    // computed in each executor. Distribute the rest evenly (if any).
    private val valuesPerSlice = numValues / numSlices
    private val slicesWithExtraItem = numValues % numSlices

    // Just ask the partition for the data
    override def compute(split: Partition, context: TaskContext): Iterator[A] =
      split.asInstanceOf[RandomPartition[A]].values

    // Generate the partitions so that the load is as evenly spread as possible
    // e.g. 10 partition and 22 items -> 2 slices with 3 items and 8 slices with 2
    override protected def getPartitions: Array[Partition] =
      ((0 until slicesWithExtraItem).view.map(new RandomPartition[A](_, valuesPerSlice + 1, random)) ++
        (slicesWithExtraItem until numSlices).view.map(new RandomPartition[A](_, valuesPerSlice, random))).toArray

  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    argument_a()
    //    argument_b()

  }

  def argument_a(): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    val df = spark.read
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      .option("delimiter", "\t")
      .load("sample.csv").toDF(
      "user_id", "item_id", "rating", "datetime"
    )
    println("===========================")
    println("Argument A: 1")
    argument_a_1(df, spark)
    println("===========================")
    println("Argument A: 2")
    argument_a_2(df, spark)
    println("===========================")
    println("Argument A: 3")
    argument_a_3(df, spark)
    println("===========================")
    println("Argument A: 4&5")
    argument_a_4_5(df, spark)
    println("===========================")
    println("Argument A: 6")
    argument_a_6(df, spark)
    println("===========================")



    //    val test = spark.sql("" +
    //      "with STEP_1 as (select user_id, max(datetime) as max_date from Data group by user_id) " +
    //      "select dt.user_id as user_id, dt.item_id as item_id, dt.rating as rating, STEP_1.max_date as max_date " +
    //      "from Data dt " +
    //      "inner join STEP_1 on dt.user_id = STEP_1.user_id and dt.datetime = STEP_1.max_date")
    //
    //    test.show(false)
    //    println(test.count())

  }

  def argument_a_1(dataFrame: DataFrame, sparkSession: SparkSession): Unit = {
    /// Argument A.1
    dataFrame.createOrReplaceTempView("Data")
    val result_1 = sparkSession.sql("" +
      "with STEP_1 as (select * from Data " +
      "where (user_id, datetime) in (select user_id, max(datetime) from Data group by user_id)) " +
      "select STEP_1.user_id as user_id, STEP_1.item_id AS last_item_id, DATEDIFF(current_date(), STEP_1.datetime) as recency " +
      "from STEP_1 " +
      "order by recency")
    result_1.show(false)
    //    println(result_1.count())
  }

  def argument_a_2(dataFrame: DataFrame, sparkSession: SparkSession): Unit = {
    /// Argument A.2
    dataFrame.createOrReplaceTempView("Data")
    val result_2 = sparkSession.sql("select user_id, count(distinct datetime) as frequency from Data group by user_id order by frequency desc")
    result_2.show(false)
    //    println(result_2.count())
  }

  def argument_a_3(dataFrame: DataFrame, sparkSession: SparkSession): Unit = {
    dataFrame.createOrReplaceTempView("Data")
    /// Argument A.3
    val result_3 = sparkSession.sql("select user_id, avg(rating) as rating from Data group by user_id")
    result_3.show(false)
    //    println(result_3.count())
  }

  def argument_a_4_5(dataFrame: DataFrame, sparkSession: SparkSession): Unit = {
    /// Argument A.4 adn Argument A.5
    dataFrame.createOrReplaceTempView("Data")
    val result_5 = sparkSession.sql("with STEP_1 AS ( with STEP_0 as ( select * from Data where (user_id, datetime) in ( select user_id, max(datetime) from Data group by user_id ) ) select STEP_0.user_id as user_id, STEP_0.item_id AS last_item_id, DATEDIFF( current_date(), STEP_0.datetime ) as recency from STEP_0 ), STEP_2 AS ( select user_id, count(distinct datetime) as frequency from Data group by user_id ), STEP_3 as ( select user_id, avg(rating) as rating from Data group by user_id ), STEP_4 AS ( SELECT STEP_2.user_id as user_id, STEP_2.frequency as frequency, STEP_1.recency as recency FROM STEP_2 LEFT JOIN STEP_1 on STEP_2.user_id = STEP_1.user_id ), STEP_5 as ( SELECT STEP_3.user_id as user_id, STEP_3.rating as rating, STEP_4.frequency as frequency, STEP_4.recency as recency FROM STEP_3 LEFT JOIN STEP_4 ON STEP_3.user_id = STEP_4.user_id ) SELECT user_id, max(rating) as rating, max(frequency) as frequency, max(recency) as recency FROM STEP_5 GROUP BY user_id")
    val discretizer = new QuantileDiscretizer()
      .setInputCols(Array("rating", "frequency", "recency"))
      .setOutputCols(Array("rating_", "frequency_", "recency_"))
      .setNumBuckets(10)

    val transformed_result = discretizer.fit(result_5).transform(result_5)
    //    transformed_result.show(false)
    transformed_result.createOrReplaceTempView("transformed_result")

    val final_result = sparkSession.sql("with STEP_1 AS (select user_id, (rating_+frequency_+recency_)/3 as RFR from transformed_result) select user_id, case when RFR>8 then 'level_1' when RFR <5 then 'level_3' else 'level_2' end as level from STEP_1 ")
    final_result.show(false)
    //    println(final_result.count())
  }

  def argument_a_6(dataFrame:DataFrame, sparkSession: SparkSession): Unit = {
    /// Argument A.6
    dataFrame.createOrReplaceTempView("Data")
    val result_6 = sparkSession.sql("WITH STEP_1 AS (select * from Data where rating > 3 order by user_id, datetime) SELECT STEP_1.USER_ID, concat_ws(',', sort_array(collect_list(struct(item_id, datetime))).item_id) AS LIST_ITEM FROM STEP_1 GROUP BY STEP_1.user_id")
    //    val result_6 = spark.sql("WITH STEP_1 AS ( select * from Data where rating > 3 order by user_id, datetime ), STEP_2 AS( SELECT STEP_1.USER_ID, concat_ws( ',', sort_array( collect_list( struct(item_id, datetime) ) ).item_id ) AS LIST_ITEM FROM STEP_1 GROUP BY STEP_1.user_id ) SELECT STEP_2.USER_ID, collect_set(LIST_ITEM) as LIST_ITEM FROM STEP_2 GROUP BY STEP_2.user_id")
    result_6.printSchema()
    result_6.show(false)
    //    println(result_6.count())
  }

  def argument_b(): Unit ={
    // Create Schema
    val arrayStructureSchema = StructType(Array(
      StructField("geoip", LongType),
      StructField("received_at", StringType),
      StructField("fields", StructType(Array(
        StructField("AppId", StringType),
        StructField("LogId", IntegerType),
        StructField("Contract", StringType)
      ))),
    ))


    val rand = new scala.util.Random
    val A = Array(51, 51, 52, 54, 55, 212, 213, 214, 215)
    val alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
    def randStr(n: Int) = (1 to n).map(_ => alpha(rand.nextInt(alpha.length))).mkString

    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    for(w <- 0 until(10)){
      val arrayStructureData = Seq.fill(1000000)(Row(rand.nextLong(), LocalDateTime.now().toString(), Row(randStr(10), rand.shuffle(A.toList).head, randStr(20))))
      val df5 = spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)
      df5.repartition(1).write.mode(SaveMode.Append).json("/home/nhatminh-it/Documents/ScalaProject/DE/data")
    }

    println("==================START MERGER=======================")

    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)
    val srcPath = new Path("data")
    val destPath = new Path("data_merged.json")
    copyMerge(hdfs, srcPath, hdfs, destPath, true, hadoopConfig)

    //Remove hidden CRC file if not needed.
    hdfs.delete(new Path(".data_merged.json.crc"), true)

  }

  def copyMerge(
                 srcFS: FileSystem, srcDir: Path,
                 dstFS: FileSystem, dstFile: Path,
                 deleteSource: Boolean, conf: Configuration
               ): Boolean = {

    if (dstFS.exists(dstFile)) {
      throw new IOException(s"Target $dstFile already exists")
    }

    // Source path is expected to be a directory:
    if (srcFS.getFileStatus(srcDir).isDirectory) {

      val outputFile = dstFS.create(dstFile)
      try {
        srcFS
          .listStatus(srcDir)
          .sortBy(_.getPath.getName)
          .collect {
            case status if status.isFile =>
              val inputFile = srcFS.open(status.getPath)
              println(inputFile)
              try {
                IOUtils.copyBytes(inputFile, outputFile, conf, false)
              }
              finally {
                inputFile.close()
              }
          }
      } finally {
        outputFile.close()
      }

      if (deleteSource) srcFS.delete(srcDir, true) else true
    }
    else false
  }

}
