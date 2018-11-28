package com.hashmap

import java.io.FileNotFoundException

import com.hashmap._
import com.hashmap.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.JsonExprUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession, types}
import org.apache.spark.sql.functions._

object Application extends App{
  val spark: SparkSession = new SparkWrapper("sample","local").getSparkObject
  try
    {
      import spark.implicits._

    val dataQuality: DataFrame = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://sandbox-hdp.hortonworks.com:8020//IndiaAffectedWaterQualityAreas.csv")
    val filteredDataQuality = dataQuality.filter(!col("State Name").contains("*") || !col("District Name").contains("*") || !col("Block Name").contains("*") || !col("Panchayat Name").contains("*") || !col("Village Name").contains("*") || !col("Habitation Name").contains("*") || !col("Quality Parameter").contains("*") || !col("Year").contains("*"))
    val cleanedQualityData = filteredDataQuality
      .withColumn("State Name",Utils.cleanCode(col("State Name")))
      .withColumn("District Name", Utils.cleanCode(col("District Name")))
      .withColumn("Block Name",Utils.cleanCode(col("Block Name")))
      .withColumn("Panchayat Name",Utils.cleanCode(col("Panchayat Name")))
      .withColumn("Village Name", Utils.cleanCode(col("Village Name")))
      .withColumn("Habitation Name",Utils.cleanCode(col("Habitation Name")))
      .withColumn("Quality Parameter", Utils.cleanCode(col("Quality Parameter")))
      .withColumn("Year",Utils.cleanCode(col("Year")))

    val dataZipcodes = spark.read.option("header","true").option("inferSchema","true").csv("hdfs://sandbox-hdp.hortonworks.com:8020//DistrictsCodes2001.csv")
    val filteredZipCodes = dataZipcodes.filter(!col("Name of the State/Union territory and Districts").contains("*"))
      .withColumn("State Code" , Utils.putDefaultValue(col("State Code")))
      .withColumn("District Code", Utils.putDefaultValue(col("District Code")))
    val cleanedZipCodesData = filteredZipCodes
      .withColumn("Name of the State/Union territory and Districts",Utils.transformUppercase(col("Name of the State/Union territory and Districts")))

    val joined = cleanedQualityData.join(cleanedZipCodesData, cleanedQualityData("District Name") === cleanedZipCodesData("Name of the State/Union territory and Districts"), "inner")
      //.filter(col("State Name")=== null)
        .select(col("State Name"),col("District Name"), col("State Code"), col("District Code"), col("Block Name"), col("Panchayat Name"), col("Village Name"), col("Habitation Name"), col("Quality Parameter"), col("Year"))
        .withColumnRenamed("State Name","state_name")
        .withColumnRenamed("District Name", "district_name")
        .withColumnRenamed("State Code","state_code")
        .withColumnRenamed("District Code","district_code")
        .withColumnRenamed("Block Name", "block_Name")
        .withColumnRenamed("Panchayat Name", "panchayat_name")
        .withColumnRenamed("Village Name","village_name")
        .withColumnRenamed("Habitation Name", "habitaion_name")
        .withColumnRenamed("Quality Parameter","quality_parameter")
      //joined.show(joined.collect().length, false)
     joined.write.format("orc").mode(SaveMode.Append).saveAsTable("WaterQuality")

      val frequencyData = joined.groupBy("village_name","quality_parameter","Year").count()
      val finaldata=frequencyData.select(
        col("village_name"),
        col("quality_parameter"),
        col("Year"),
        col("count").alias("frequency"))
        .withColumn("Year", Utils.extractYear(col("Year")))
      finaldata.filter(col("village_name") === "EAST GODAVARI").show()
      //finaldata.show(finaldata.collect().length, false)
      finaldata.write.format("orc").mode(SaveMode.Append).saveAsTable("frequency")

    }catch
      {
        case ex: NullPointerException => throw new NullPointerException("NullPointerException in VesselEncroachment"+ex.getMessage)
        case ex: FileNotFoundException => throw new FileNotFoundException(" File Not Found"+ex.getMessage)
        case ex: Exception => throw new Exception(ex.getMessage)
      }


}
