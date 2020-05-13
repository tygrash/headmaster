import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.zalando.spark.jsonschema.SchemaConverter

import scala.util.parsing.json.JSONObject

object Main {
  private val JSON_SCHEMA_STRING =
    """{
      |    "title": "Floor Access Event",
      |    "type": "object",
      |    "properties": {
      |        "person_id": {
      |            "type": "string"
      |        },
      |        "datetime": {
      |            "type": "string",
      |            "format": "date-time"
      |        },
      |        "floor_level": {
      |            "type": "integer"
      |        },
      |        "building": {
      |            "type": "string"
      |        }
      |    },
      |    "required": ["person_id", "datetime", "floor_level", "building"]
      |}""".stripMargin

  def createSparkSession(): SparkSession = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark
  }

  def createSchemafromJSON(jsonSchemaString: String): StructType = {
    SchemaConverter.convertContent(jsonSchemaString)
  }

  def readCSVToDF(sparkSession: SparkSession, schema: StructType, CSVPath: String): DataFrame = {
    sparkSession.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(CSVPath)
  }

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

  def main(args: Array[String]): Unit = {
    val sparkSession = createSparkSession()

    val schema = createSchemafromJSON(JSON_SCHEMA_STRING)
    val csv_df_with_schema = readCSVToDF(sparkSession, schema, "src/main/resources/data.csv")

    csv_df_with_schema.foreach { row => println(convertRowToJSON(row)) }
  }
  
}