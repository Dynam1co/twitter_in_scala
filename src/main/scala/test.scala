import org.apache.spark.{SparkConf, SparkContext}

object test {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        conf.setAppName("LearnScalaSpark")
        conf.setMaster("local")

        val sc = new SparkContext(conf)

        sc.setLogLevel("OFF")

        val helloWorldString = "Hello World!"
        print(helloWorldString)
    }
}
