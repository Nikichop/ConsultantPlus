package org.example

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.expressions.Window
import scala.collection.mutable

object Task1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ConsultantAnalyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val targetId = "ACC_45616"

    // Чтение лог-данных через общий модуль
    val logData = CommonUtils.readLogData(spark, "sessions/*")

    // Окно для отслеживания состояния карточки
    val windowSpec = Window.orderBy("line_number")

    // Обработка строк, связанных с карточкой поиска
    val cardData = logData
      .withColumn("is_card_start", F.col("line").startsWith("CARD_SEARCH_START"))
      .withColumn("is_card_end", F.col("line").startsWith("CARD_SEARCH_END"))
      .withColumn("card_flag",
        F.sum(
          F.when(F.col("is_card_start"), 1)
            .when(F.col("is_card_end"), -1)
            .otherwise(0)
        ).over(windowSpec)
      )

    // Фильтрация строк внутри карточек
    val filteredData = cardData
      .filter(F.col("card_flag") > 0 || F.col("is_card_start"))
      .filter(!F.col("line").startsWith("CARD_SEARCH_"))

    // Подсчет вхождений целевого идентификатора
    val accCount = filteredData
      .select("line", "filename")
      .as[(String, String)]
      .map { case (line, filename) =>
        val count = line.split("\\s+").count(_ == targetId)
        (filename, count)
      }
      .rdd
      .reduceByKey(_ + _)
      .filter(_._2 > 0)
      .collect()

    // Формирование результата
    val totalCount = accCount.map(_._2).sum
    val result = mutable.Map[String, Any](
      "document_id" -> targetId,
      "total_count" -> totalCount,
      "file_counts" -> accCount.map { case (f, c) =>
        Map("filename" -> f, "count" -> c)
      }
    )

    // Получаем форматированный JSON через общий модуль
    val json = CommonUtils.getPrettyJson(result)

    // Сохраняем результат в файл
    CommonUtils.writeJsonToFile(json, "task1_results", "result")

    spark.stop()
  }
}
