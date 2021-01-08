//import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

object invertedIndex {
    def main(args: Array[String]): Unit = {
        // val spark = SparkSession.builder.master("local[2]").appName("Simple Application").getOrCreate()
        val sc = new SparkContext("local[*]", "invertedIndex")
        val skFiles = sc.wholeTextFiles("/tmp/shakespeare") // load files from the directory
        println(skFiles.count())
    }
}