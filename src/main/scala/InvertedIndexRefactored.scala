import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object InvertedIndexRefactored {

    val stopWords: Array[String] = Array("to", "me")

    def keep(word: String): Boolean = {
        !stopWords.contains(word)
    }

    def main(args: Array[String]): Unit = {
        val sc = new SparkContext("local[*]", "invertedIndexRefactored")
        val spark = SparkSession.builder().master("local[*]")
            .appName("invertedIndexRefactored")
            .getOrCreate()

        val invertedIndex = sc.wholeTextFiles("./shakespeare")
            .flatMap {
                case (location, contents) => {
                    val words = contents.split("""\W+""").filter(word => word.size > 0)
                        .filter(keep)
                    val filename = location.split("/").last
                    words.map(word => ((word, filename), 1))
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

//        val examples = invertedIndex.take(10)
//        examples.foreach(println)

        val topLocationsLoveHate = spark.sql(
    """
      |WITH relevant_words AS (
      |SELECT word, total_count, locations[0] AS top_location, counts[0]
      |FROM inverted_index
      |WHERE (word LIKE '%love%') OR (word LIKE '%hate%')
      |)
      |SELECT * FROM relevant_words
      |WHERE word NOT IN ('glove', 'gloves', 'whate', 'whatever', 'Whatever')
      |""".stripMargin)

        topLocationsLoveHate.show()

    }
}
