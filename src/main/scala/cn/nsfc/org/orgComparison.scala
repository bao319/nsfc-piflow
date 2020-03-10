package cn.nsfc.org


import org.apache.spark.sql.{DataFrame, SparkSession}

object orgComparison {
  val spark = SparkSession.builder()
    .master("local[12]")
    .appName("packone_139")
    .config("spark.deploy.mode","client")
    .config("spark.driver.memory", "15g")
    .config("spark.executor.memory", "32g")
    .config("spark.cores.max", "16")
    .config("hive.metastore.uris","thrift://192.168.3.140:9083")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    orgToHive
  }

  def orgToHive(): Unit = {
    import spark.implicits._

    val rdd = spark.sparkContext.textFile("file:///yg/final_1120.csv")
    println(rdd.count())

    val frame: DataFrame = rdd.map(x => {
      (x.split("\t")(0), x.split("\t")(1))
    }).toDF("origin_name", "standard_name")
    println(frame.count())
    frame.createOrReplaceTempView("frame")
    frame.show()

//    spark.sql(s"insert into middle.m_org_comparison_part select * from frame")


  }

}
