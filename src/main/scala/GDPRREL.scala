import com.sun.corba.se.impl.orb.ORBConfiguratorImpl.ConfigParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scopt.OptionParser


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
//    def hashuser(id: Int, Data: DataFrame): DataFrame = {
//      person.withColumn("id",
//        base64(bin(hash("first_name","last_name","email", "gender", "ip_address","Job Title", "Company Name","University"   ))))
//      )
//
//    }
    //val hashDf = hashuser(10, person)

    val haseddDf = DFperson.withColumn("first_name",
                   when(col("id") ==="10", lit(md5(  col("first_name"  ))))
                     .otherwise(col("first_name"))

    )



    haseddDf.show()
    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("scopt"),
        head("scopt", "4.x"),
        // option -f, --foo
        opt[Int]('f', "foo")
          .action((x, c) => c.copy(foo = x))
          .text("foo is an integer property"),
        // more options here...
      )
    }
  }
}
