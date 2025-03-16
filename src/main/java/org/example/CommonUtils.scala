package org.example

import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}
import java.io.{File, PrintWriter}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object CommonUtils {

  // Чтение лог-файлов с использованием Spark DataFrame (для Task1)
  def readLogData(spark: SparkSession, path: String = "sessions/*"): DataFrame = {
    spark.read
      .option("encoding", "Windows-1251")
      .text(path)
      .toDF("line")
      .withColumn("filename", F.regexp_extract(F.input_file_name(), "([^/]+)$", 1))
      .withColumn("line_number", F.monotonically_increasing_id())
  }

  // Чтение лог-файлов с нумерацией строк через RDD (для Task2)
  def readRawDataWithIndex(spark: SparkSession, path: String = "sessions/*"): DataFrame = {
    import spark.implicits._
    spark.sparkContext.textFile(path)
      .zipWithIndex()
      .map { case (line, idx) => (line, idx) }
      .toDF("line", "idx")
  }

  // Сохранение форматированного JSON в файл
  def writeJsonToFile(json: String, outputDir: String, filePrefix: String): Unit = {
    val dir = new File(outputDir)
    if (!dir.exists()) dir.mkdir()
    val timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"))
    val outputFile = new File(dir, s"${filePrefix}_$timestamp.json")
    val writer = new PrintWriter(outputFile)
    writer.write(json)
    writer.close()
    println(s"Результаты сохранены: ${outputFile.getAbsolutePath}")
  }

  // Преобразование объекта в форматированный JSON
  def getPrettyJson[T <: AnyRef](obj: T): String = {
    import org.json4s._
    import org.json4s.jackson.Serialization
    implicit val formats: Formats = Serialization.formats(NoTypeHints)
    Serialization.writePretty(obj)
  }

}
