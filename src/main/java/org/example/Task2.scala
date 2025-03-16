package org.example

import org.apache.spark.sql.{SparkSession, functions => F}
import org.example.CommonUtils

object Task2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DocumentOpenAnalyzer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Чтение данных с нумерацией строк через общий модуль
    val rawData = CommonUtils.readRawDataWithIndex(spark, "sessions/*")

    // Извлечение QS-событий (строки, начинающиеся с "QS ")
    val qsLines = rawData.filter($"line".startsWith("QS "))

    // DataFrame для следующей строки (с перечнем документов)
    val nextLineDF = rawData.select($"line".as("next_line"), $"idx".as("next_idx"))

    // Объединяем QS-строки с последующими строками (условие: idx + 1 == next_idx)
    val qsWithDocs = qsLines.join(
        nextLineDF,
        qsLines("idx") + F.lit(1) === nextLineDF("next_idx")
      )
      .select(
        qsLines("line").as("qs_line"),
        $"next_line"
      )
      // Извлекаем дату QS и список документов
      .withColumn("qs_date", F.split(F.col("qs_line"), " ")(1).substr(0, 10))
      .withColumn("document_ids", F.split($"next_line", " "))
      .select("qs_date", "document_ids")

    // Разворачиваем массив идентификаторов документов и фильтруем мусорные значения
    val qsData = qsWithDocs
      .withColumn("document_id", F.explode($"document_ids"))
      .filter(
        $"document_id" =!= "" &&
          !$"document_id".contains("{") &&
          !$"document_id".contains("}")
      )
      .select("qs_date", "document_id")

    // Обработка событий DOC_OPEN: выбираем строки, начинающиеся с "DOC_OPEN "
    val docOpenData = rawData.filter($"line".startsWith("DOC_OPEN "))
      .select(
        F.split($"line", " ")(1).substr(0, 10).as("open_date"),
        F.split($"line", " ")(3).as("document_id")
      )

    // Соединяем QS и DOC_OPEN по документу и дате (один и тот же день)
    val joined = qsData.join(docOpenData,
      qsData("document_id") === docOpenData("document_id") &&
        qsData("qs_date") === docOpenData("open_date"),
      "inner"
    )

    // Группируем по документу и дате для подсчета открытий
    val agg = joined.groupBy(qsData("document_id"), qsData("qs_date"))
      .agg(F.count("*").as("opens"))

    // Формируем итоговую карту: для каждого документа Map(date -> opens)
    val result = agg.groupBy("document_id")
      .agg(F.map_from_entries(F.collect_list(F.struct($"qs_date", $"opens"))).as("daily_opens"))
      .collect()

    // Приводим результат к формату, удобному для JSON-сериализации
    val output = result.map { row =>
      val documentId = row.getString(0)
      val dailyOpens = row.getMap[String, Long](1)
      Map(
        "document_id" -> documentId,
        "daily_opens" -> dailyOpens
      )
    }

    // Получаем форматированный JSON через общий модуль
    val json = CommonUtils.getPrettyJson(output)

    // Сохраняем результат в файл
    CommonUtils.writeJsonToFile(json, "task2_results", "result")

    spark.stop()
  }
}
