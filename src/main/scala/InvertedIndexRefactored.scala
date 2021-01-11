import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object InvertedIndexRefactored {

    val stopWords: Set[String] = Set("to", "me", "", "a", "and", "he", "it", "not", "the","my", "but", "will", "be", "I", "you", "in", "is", "are", "with", "for", "that", "of", "his", "her", "so", "s", "your", "sir", "and", "this", "as", " and", "have", "him", "what", "thou", "no", "if", "do", "by", "she", "d", "we", "our", "thee", "shall", "sir")

    def keep(word: String): Boolean = !stopWords.contains(word)

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext("local[*]", "invertedIndexRefactored")
        val spark = SparkSession.builder().master("local[*]")
            .appName("invertedIndexRefactored")
            .getOrCreate()

        val invertedIndex = sc.wholeTextFiles("./source_files")
            .flatMap {
                case (location, contents) => {
                    val words = contents.split("""\W+""").filter(word => word.size > 0)
                        .filter(keep)
                    val filename = location.split("/").last
                    words.map(word => ((word.toLowerCase(), filename), 1))
                }
            }
            .reduceByKey((acc, num) => acc + num)
            .map{
                case ((word, filename), count) => (word, (filename, count))
            }
            .groupByKey().sortByKey(ascending = true)
            .map { case (word, iterable) => // pattern matching on a whole record
                val vec = iterable.toVector.sortBy { case (filename, count) => (-count, filename) }
                val (locations, counts) = vec.unzip // separate filenames and counts into their own vectors
                val totalCount = counts.reduceLeft( (n1, n2) => n1 + n2) // calculate the total count

                (word, totalCount, locations, counts) // new tuple
            }

        val iiDF = spark.createDataFrame(invertedIndex).toDF("word","total_count","locations","counts")
        iiDF.cache
        iiDF.createOrReplaceTempView("inverted_index") // now sql queries can be run

        val top10Words = spark.sql(
    """
      |SELECT word, total_count, locations[0] AS top_location, counts[0] AS count
      |FROM inverted_index
      |ORDER BY total_count DESC
      |LIMIT 10
      |""".stripMargin)

        top10Words.show()

    }
}
