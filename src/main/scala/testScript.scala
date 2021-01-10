object testScript extends App {

    def patternMatch(tup: (String, Int)): Unit ={
        tup match {
            case (name, age) => {
                val myStr =
                    s"""
                       |---------------
                       | Name is $name
                       | Age  is $age
                       | ---------------
                       |""".stripMargin
                println(myStr)
            }
        }
    }

    val data = Array(("masa", 23), ("take", 22), ("taka", 22))
    val dataInverted = data.map{
        case (name, age) => (age, name)
    }
    dataInverted.foreach(println)



}