import java.text.SimpleDateFormat

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  val BIDS_DIVIDED_DIR: String = "bids_divided"
  val BIDS_PATH = "spark2/src/main/resources/bids_parquet";
  val MOTELS_PATH = "spark2/src/main/resources/motels_parquet";
  val EXCHANGE_RATE = "spark2/src/main/resources/rates_parquet"
  val OUTPUT = "spark2/src/main/resources/output"

  def main(args: Array[String]): Unit = {
    //  require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")
    System.setProperty("hadoop.home.dir", "C:\\hadoop_home")
    val bidsPath = BIDS_PATH
    //args(0)
    val motelsPath = MOTELS_PATH
    //args(1)
    val exchangeRatesPath = EXCHANGE_RATE
    //args(2)
    val outputBasePath = OUTPUT //args(3)


    val sc = SparkSession.builder.
      master("local")
      .appName("sparkCOre") //.config("spark.sql.warehouse.dir", "./spark-warehouse")
      .getOrCreate().sparkContext
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }
  def convertFromCSVtoParquet(sqlContext: HiveContext,bidsPath: String, motelsPath: String, exchangeRatesPath: String) = {
    val df1 = getRawBids(sqlContext,bidsPath)
    df1.write.parquet("bids_parquet")
    val df2 = getMotels(sqlContext,motelsPath)
    df2.write.parquet("motels_parquet")
    val df3 = getExchangeRates(sqlContext,exchangeRatesPath)
    df3.write.parquet("rates_parquet")
  }
  def processData(sqlContext: HiveContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(sqlContext, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    //erroneousRecords.write.parquet(s"$outputBasePath/$ERRONEOUS_DIR")
    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)
    exchangeRates.cache()
    //exchangeRates.collect().foreach(println)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    // val convertDate: UserDefinedFunction = getConvertDate
    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(sqlContext, rawBids, exchangeRates)
    //bids.printSchema()
    //bids.show()
    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(sqlContext, motelsPath)
    motels.cache()
    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.show()
   //enriched.write.parquet(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    //val df: DataFrame = sqlContext.read.option("header","false").csv(bidsPath)
    val df: DataFrame = sqlContext.read.parquet(bidsPath).toDF("MotelID", "BidsDate", "HU", "UK",  "NL", "US", "MX", "AU", "CA", "CN", "KR","BE", "I","JP", "IN", "HN", "GY", "DE")
    df
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    val errorsRecords = rawBids.filter(rawBids("HU").contains("ERROR")).groupBy("BidsDate", "HU").count()
    errorsRecords
  }

  def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
    //val df = sqlContext.read.option("header","false").csv(exchangeRatesPath)
    val df = sqlContext.read.parquet(exchangeRatesPath).toDF("BidsDate", "CurrencyName", "CurrencyCode", "ExchangeRate").drop("CurrencyName").drop("CurrencyCode")
    df
  }


  val getConvertDate = udf((date1: String) =>
    new SimpleDateFormat("yyyy-MM-dd HH:mm").format(new SimpleDateFormat("HH-dd-MM-yyyy").parse(date1)))
  val getRoundedTo3 = udf((price:Double) => BigDecimal(price).setScale(3, BigDecimal.RoundingMode.HALF_UP))
  val getConcatenated = udf((first: String, second: String, third: String) =>
    (if (first == null) "null" else first) + "," + (if (second == null) "null" else second) + "," + (if (third == null) "null" else third))

  def getBids(sqlContext: HiveContext, rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = {
    import sqlContext.implicits._
    val filteredBids = rawBids.filter(!rawBids("HU").contains("ERROR"))
      .select(Constants.TARGET_LOSAS.head, Constants.TARGET_LOSAS.tail: _*).toDF()

    val df = filteredBids.select(filteredBids("MotelID"), filteredBids("BidsDate"), getConcatenated(filteredBids("US"), filteredBids("MX"), filteredBids("CA")).alias("Losas"))
    val flatten_df = df.flatMap(row => {
      val motel_id = row.get(0).toString
      val date = row.get(1).toString
      val locations = row.get(2).toString.split(",")
      var m = scala.collection.mutable.Map[String, String]()
      for (i <- 0 to 2) {
        if (!locations(i).equals("null"))
          m += (Constants.LOSAS(i) -> locations(i))
      }
      val seq = for ((k, v) <- m) yield {
        (motel_id, date, k, v)
      }
      seq
    }).toDF("MotelID", "BidsDate", "Losa", "Price")
      .join(exchangeRates,"BidsDate").map(x=>{
      // convert USD (third column) to EUR (fourth column)
      val res = x.get(3).toString.toDouble*x.get(4).toString.toDouble
      (x.get(1).toString, x.get(0).toString,x.get(2).toString,BigDecimal(res).setScale(3, BigDecimal.RoundingMode.HALF_UP)                                                                               )
    }).drop("ExchangeRate").toDF("MotelID", "BidsDate", "Losa", "Price")

    flatten_df.select(flatten_df("MotelID"), getConvertDate(flatten_df("BidsDate")).alias("BidsDate"), flatten_df("Losa"), flatten_df("Price"))
  }


  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = {
    import sqlContext.implicits._
    //val df: DataFrame = sqlContext.read.option("header","false").csv(motelsPath)
    val df: DataFrame = sqlContext.read.parquet(motelsPath).map(x=>Tuple2(x.get(0).toString,x.get(1).toString)).toDF("MotelID", "MotelName")
    df
  }
  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = {
    val joined = bids.join(motels,"MotelID")
  //  joined.select(joined("MotelID"), joined("BidsDate"), joined("Price")).where(joined("MotelID")==="0000009").where(joined("BidsDate")==="2016-05-06 09:00")
   // joined.groupBy("MotelID","BidsDate")//.max("Price").select(joined("MotelName"))
   //val w = Window.orderBy("BidsDate","MotelID")
    val wSpec3 = Window.partitionBy("MotelID","BidsDate").orderBy("Price")
    val resultDF = joined.withColumn("Ranks", rank.over(wSpec3))
    resultDF.select(resultDF("MotelID"),resultDF("MotelName"),resultDF("BidsDate"),resultDF("Losa"),getRoundedTo3(resultDF("Price")).alias("Price")).where(resultDF("Ranks")===3)
  }
}
