import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  val BIDS_DIVIDED_DIR: String = "bids_divided"
  val BIDS_PATH = "spark2/src/main/resources/bids.txt";
  val MOTELS_PATH = "SparkSQL/src/main/resources/motels.txt";
  val EXCHANGE_RATE = "spark2/src/main/resources/exchange_rate.txt"
  val OUTPUT = "spark2/src/main/resources/output"

  def main(args: Array[String]): Unit = {
    //  require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = BIDS_PATH
    //args(0)
    val motelsPath = MOTELS_PATH
    //args(1)
    val exchangeRatesPath = EXCHANGE_RATE
    //args(2)
    val outputBasePath = OUTPUT //args(3)


    val sc = SparkSession.builder.
      master("local")
      .appName("spark-sql").config("spark.sql.warehouse.dir", "./spark-warehouse")
      .getOrCreate().sparkContext
    val sqlContext = new HiveContext(sc)

    processData(sqlContext, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
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
    erroneousRecords.write.format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")
    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(sqlContext, exchangeRatesPath)
    //exchangeRates.collect().foreach(println)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    /* val convertDate: UserDefinedFunction = getConvertDate

     /**
       * Task 3:
       * Transform the rawBids
       * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
       * - Convert dates to proper format - use formats in Constants util class
       * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
       */
     val bids: DataFrame = getBids(rawBids, exchangeRates)

     /**
       * Task 4:
       * Load motels data.
       * Hint: You will need the motels name for enrichment and you will use the id for join
       */
     val motels: DataFrame = getMotels(sqlContext, motelsPath)

     /**
       * Task5:
       * Join the bids with motel names.
       */
     val enriched: DataFrame = getEnriched(bids, motels)
     enriched.write
       .format(Constants.CSV_FORMAT)
       .save(s"$outputBasePath/$AGGREGATED_DIR")*/
  }

  def getRawBids(sqlContext: HiveContext, bidsPath: String): DataFrame = {
    val df: DataFrame = sqlContext.read.format(Constants.CSV_FORMAT).schema(Constants.BIDS_HEADERS).load(bidsPath)
    df
  }

  def getErroneousRecords(rawBids: DataFrame): DataFrame = {
    val errorsRecords = rawBids.filter(rawBids("HU").contains("ERROR")).groupBy("BidsDate", "HU").count()
    errorsRecords
  }

   def getExchangeRates(sqlContext: HiveContext, exchangeRatesPath: String): DataFrame = {
     val df = sqlContext.read.csv(exchangeRatesPath)
     df
   }


  /*def getConvertDate: UserDefinedFunction = ???

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame): DataFrame = ???

  def getMotels(sqlContext: HiveContext, motelsPath: String): DataFrame = ???

  def getEnriched(bids: DataFrame, motels: DataFrame): DataFrame = ???*/
}
