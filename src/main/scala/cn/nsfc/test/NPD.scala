package cn.nsfc.test

import org.apache.spark.sql.SparkSession

object NPD {

  val spark = SparkSession.builder()
    .master("local[12]")
    .appName("packone_139")
    .config("spark.deploy.mode","client")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "8g")
    .config("spark.cores.max", "8")
    .config("hive.metastore.uris","thrift://192.168.3.140:9083")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    csvToHive()
  }



  def csvToHive(): Unit = {
    import spark.implicits._


    val rdd = spark.sparkContext.textFile("file:///yg/project_prjno.txt")
    println(rdd.count())


    val frame = rdd.toDF("prj_no")
    frame.createOrReplaceTempView("frame")


    val project_npd = spark.sql(
      s"""
         | select projectno,gname,reccompletiondateyear  from npd_fund.project2019_trim where  reccompletiondateyear <2017
         |
      """.stripMargin)

    println(project_npd.count())

    project_npd.createOrReplaceTempView("project_npd")

    val project_npd_not_exists =  spark.sql(
      """
        |select * from project_npd a  where not exists (select * from frame b where trim(a.projectno)=trim(b.prj_no))
      """.stripMargin)


    project_npd_not_exists.show()
    println(project_npd_not_exists.count())

    project_npd_not_exists.createOrReplaceTempView("project_npd_not_exists")


    spark.sql(
      """
        |select a.*,b.grant_code,b.grant_name from project_npd_not_exists a left join origin.o_project b on trim(a.projectno)=trim(b.prj_no)
      """.stripMargin).createOrReplaceTempView("temp")

    spark.sql("select reccompletiondateyear, count(*) from temp group by reccompletiondateyear order by reccompletiondateyear").show()





  }

}
