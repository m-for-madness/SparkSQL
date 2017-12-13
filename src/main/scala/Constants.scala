import org.apache.spark.sql.types._
import org.joda.time.format.DateTimeFormat

object Constants {

  val DELIMITER = ","

  val CSV_FORMAT = "com.databricks.spark.csv"

  val EXCHANGE_RATES_HEADER = StructType(Array("ValidFrom", "CurrencyName", "CurrencyCode", "ExchangeRate")
    .map(field => StructField(field, StringType, true)))
  val BIDS_HEADERS = StructType(Array(StructField("MotelID", StringType)
    , StructField("BidsDate", StringType)
    , StructField("HU", StringType)
    , StructField("UK", DoubleType)
    , StructField("NL", DoubleType)
    , StructField("US", DoubleType)
    , StructField("MX", DoubleType)
    , StructField("AU", DoubleType)
    , StructField("CA", DoubleType)
    , StructField("CN", DoubleType)
    , StructField("KR", DoubleType)
    , StructField("BE", DoubleType)
    , StructField("I", DoubleType)
    , StructField("JP", DoubleType)
    , StructField("IN", DoubleType)
    , StructField("HN", DoubleType)
    , StructField("GY", DoubleType)
    , StructField("DE", DoubleType)))


  val TARGET_LOSAS = Seq("US", "CA", "MX")

  val INPUT_DATE_FORMAT = DateTimeFormat.forPattern("HH-dd-MM-yyyy")
  val OUTPUT_DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm")
}
