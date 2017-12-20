import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","

  val CSV_FORMAT = "com.databricks.spark.csv"

  val EXCHANGE_RATES_HEADER = StructType(Array("BidsDate", "CurrencyName", "CurrencyCode", "ExchangeRate")
    .map(field => StructField(field, StringType, true)))
  val BIDS_HEADERS = StructType(Array(StructField("MotelID", StringType)
    , StructField("BidsDate", StringType)
    , StructField("HU", StringType)
    , StructField("UK", StringType)
    , StructField("NL", StringType)
    , StructField("US", StringType)
    , StructField("MX", StringType)
    , StructField("AU", StringType)
    , StructField("CA", StringType)
    , StructField("CN", StringType)
    , StructField("KR", StringType)
    , StructField("BE", StringType)
    , StructField("I", StringType)
    , StructField("JP", StringType)
    , StructField("IN", StringType)
    , StructField("HN", StringType)
    , StructField("GY", StringType)
    , StructField("DE", StringType)))

  val MOTELS_HEADERS = Seq("MotelID", "MotelName", "Abb", "Comments")
  val TARGET_LOSAS = Seq("MotelID","BidsDate","US", "CA", "MX")
  val LOSAS = Seq("US", "CA", "MX")
  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
}
