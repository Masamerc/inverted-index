import org.apache.spark.SparkContext

object invertedIndex {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext("local[*]", "invertedIndex")
        val skFiles = sc.wholeTextFiles("./shakespeare") // load files from the directory

        val wordFileNameOnes = skFiles.flatMap{ tup =>
            // example record / tuple in skFiles: (filename, "all the words")
            val words = tup._2.split("""\W+""")
            val filename = tup._1.split("/").last
            words.map( word => ((word, filename), 1))
        }

        val uniques = wordFileNameOnes.reduceByKey((acc, count) => acc + count) // sum the count for each key: (word, filename)

        val wordsAsIndex = uniques.map {tup => // change the order / grouping within the tuple
            (tup._1._1, (tup._1._2, tup._2))
        }

        val wordGroups = wordsAsIndex.groupByKey.sortByKey(ascending = true) // aggregate (filename, count) for each key: group

        val examples = wordGroups.take(50)
        examples.foreach(println)
    }
}