
import org.apache.spark.sql.SparkSession

object Main {
    def main(args: Array[String]) {
        val spark = SparkSession.builder.master("local[2]").appName("Simple Application").getOrCreate()
        val data = spark.read.option("header", "true").csv("/tmp/data.csv")
        data.show()
    }
}