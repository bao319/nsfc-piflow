package cn.nsfc.test

import org.apache.spark.sql.SparkSession

object NPD_1 {

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


    val rdd = spark.sparkContext.textFile("file:///yg/neo4j.csv")
    println(rdd.count())


    // 343579
    val frame = rdd.toDF("prj_no")
    frame.createOrReplaceTempView("frame")
    println(frame.count())

//    spark.sql(
//      """
//        |insert into middle.m_prj_no_neo4j  select * from frame
//      """.stripMargin)

    println("------")


    while (true){

    }




    val df_prj_no = spark.sql(
      """
        | select * from (
        |  select trim(prj_code) as prj_code,trim(a.prj_no) as prj_no,final_year from frame a left join middle.m_project b on trim(a.prj_no)=trim(b.prj_no)
        |)a where  final_year is null
      """.stripMargin)
    println("df_prj_no           "+df_prj_no.count())


    df_prj_no.createOrReplaceTempView("df_prj_no")




    // 38707            prj_code   38705
    val project_rpt = spark.sql(
      """
        |select * from (
        |select  a.prj_code,prj_no,b.id ,b.rpt_year from   df_prj_no  a left join origin.o_rpt_schedule b
        | where a.prj_code = b.prj_code and b.prj_code  is not null and b.status = '07' and rpt_type = '4'  and enable= '1' )a
        | group by prj_code,prj_no ,id,rpt_year
      """.stripMargin)

    project_rpt.createOrReplaceTempView("project_rpt")

        println(project_rpt.count())


    val project_rpt_pdf = spark.sql(
      """
        |select prj_code,prj_no,a.id,b.pdf_file_code ,last_confirm_date,confirm_date from project_rpt a
        | left join origin.o_rpt_completion_20191112 b
        |  on a.id = b.rpt_id  where pdf_file_code is not null
      """.stripMargin)
    project_rpt_pdf.createOrReplaceTempView("project_rpt_pdf")
        println(project_rpt_pdf.count())


    // 38705
    val project_rpt_pdf_dis = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by prj_code order by last_confirm_date desc  ) rank from project_rpt_pdf )a
        | where rank =1
      """.stripMargin).drop("rank").drop("last_confirm_date")
    project_rpt_pdf_dis.createOrReplaceTempView("project_rpt_pdf_dis")

    println(project_rpt_pdf_dis.count())

    project_rpt_pdf_dis.show()

    println(spark.sql("select * from project_rpt_pdf_dis where confirm_date is not null").count())

    spark.sql("select * from project_rpt_pdf_dis where confirm_date is not null").show()








  }

}
