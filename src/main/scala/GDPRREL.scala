import com.sun.corba.se.impl.orb.ORBConfiguratorImpl.ConfigParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import spray.json._
import scopt.OptionParser
import DefaultJsonProtocol._
import java.io.File


object GDPRREL {

  val schema = StructType(Array(
    StructField("id",IntegerType, false),
    StructField("first_name", StringType, false),
    StructField("last_name", StringType, false),
    StructField("email",StringType, false),
    StructField("gender",StringType,false),
    StructField("ip_address",StringType,false),
    StructField("Job Title",StringType,false),
    StructField("Company Name",StringType,false),
    StructField("University", StringType,false)))


  def readData(path: String, fileType: String, structure: StructType)(implicit sparkSession: SparkSession): Either[String, DataFrame] = {
    fileType match {
      case "CSV" => Right(sparkSession.read.schema(structure).option("header", true).csv(path))
      case "PARQUET" => Right(sparkSession.read.parquet(path))
      case _ => Left("File format is not allowed")
    }
  }
  def writeData(df: DataFrame, outputPath: String, outputFormat: String) = {
    if(outputFormat == "CSV") {
      df.coalesce(1).write.mode("overwrite").csv(outputPath)
    }
    else {
      df.coalesce(1).write.mode("overwrite").parquet(outputPath)
    }
  }
  def deleteuser(id: Int, Data: DataFrame): DataFrame = {
    Data.filter(col("id").notEqual(id))

  }
  def hashIdColumn(df: DataFrame, id: Int, columnNameToHash: String): DataFrame ={
    df.withColumn(columnNameToHash,
      when(col("id") ===id, lit(md5(  col(columnNameToHash  ))))
        .otherwise(col(columnNameToHash))
    )
  }
  def extractuser(id: Int, df: DataFrame): Unit={
    val dfid = df.filter(col("id").equalTo(id))
    dfid.write.option("header",true).csv("/Users/tarikbelattar/Documents/Spark/Data/GDPR_RESULT/Extract_user_"+id+".csv")
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    implicit val sparkSession: SparkSession = SparkSession.builder().master("local").getOrCreate()
    import sparkSession.implicits._
    var path = "/Users/tarikbelattar/Documents/Spark/Data/MOCK_DATA.csv"
    val personas  = readData(path, "CSV", schema)
    //personas.right.get.show()
    val personnes = personas.right.get
    personnes.show()
    personnes.printSchema()

    println("################GDPR#####################")
    println("################SUPPRIMER LES DONNÉES D'UN USER#####################")

    val df = deleteuser(15,personnes)
    df.show()
    val dfsave = writeData(df, "/Users/tarikbelattar/Documents/Spark/Data/GDPR", "CSV")

    println("################GDPR#####################")
    println("################HASHER LES DONNÉES D'UN USER#####################")
    val person = readData(path,"CSV", schema)
    val DFperson = person.right.get

    val hasheduserLN = hashIdColumn(DFperson,11,"last_name")
    val hasheduserLNFN = hashIdColumn(hasheduserLN,11,"first_name")
    val hasheduserLnFnEm = hashIdColumn(hasheduserLNFN,11,"email")
    hasheduserLnFnEm.show()

    println("################GDPR#####################")
    println("################GÉNERER LES DONNÉES D'UN USER DANS UN FICHIER CSV#####################")
    val dfgeneratedData = extractuser(15, DFperson)
    }
  }

