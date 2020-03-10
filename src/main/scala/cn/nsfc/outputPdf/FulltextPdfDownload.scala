package cn.nsfc.outputPdf

import cn.nsfc.util.DownloadFromUrl
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql._


class FulltextPdfDownload extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "FulltextPdfDownload"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)


  var final_year:String = _
  var fulltext_pdf_table:String = _
  var hdfsUrl:String = _
  var pdfDir:String = _
  var isisnUrl:String = _

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    println("start------------------------------FulltextPdfDownload")
    val spark = pec.get[SparkSession]()

    //      采集 project 表中，年份为final_year ，状态为已结题(30)的数据
    val project_rpt_schedule = spark.sql(
      s"""
         |select * from (
         |   select  a.prj_code,prj_no,b.id  from  origin_piflow.o_project a left join origin_piflow.o_rpt_schedule b
         |   on  a.prj_code = b.prj_code
         |   where b.prj_code  is not null and b.status = '07' and b.rpt_type = '4'  and b.enable= '1'  and a.status = 30 and a.final_year = ${final_year}
         | )a  group by prj_code,prj_no ,id
        """.stripMargin)
    project_rpt_schedule.createOrReplaceTempView("project_rpt_schedule")


    // 取出结题报告的 pdf_file_code
    val project_pdf = spark.sql(
      """
        |select * from (
        | select *, row_number() over (partition by prj_code order by last_confirm_date desc  ) rank from  (
        |   select prj_code,prj_no,a.id,b.pdf_file_code ,last_confirm_date from project_rpt_schedule a left join origin_piflow.o_rpt_completion b
        |   on a.id = b.rpt_id where pdf_file_code is not null )a
        | )b  where rank =1
      """.stripMargin).drop("rank").drop("last_confirm_date").cache()
    project_pdf.createOrReplaceTempView("project_pdf")




    println("---------------------------------project_product_pdf-----------------------------")
    // 结题报告对应的所有成果
    val project_product =  spark.sql(
      """
        |select * from (
        |   select id ,product_id ,key_code,product_type  from middle_piflow.m_product_business a where exists (select * from project_pdf b where a.key_code= b.id)
        |)a group by id ,product_id ,key_code,product_type
      """.stripMargin).cache()
    project_product.createOrReplaceTempView("project_product")
    println(project_product.count())

    // 有全文的成果
    val type3_4 = spark.sql(
      """
        |select * from (
        |select id,full_text_file_code, '3' as pub_type_id  from  temp_piflow.t_product_business_ext_type3 where full_text_file_code is not null
        | union
        | select id,full_text_file_code,'4' as pub_type_id from  temp_piflow.t_product_business_ext_type4 where full_text_file_code is not null
        | )a group by id,full_text_file_code,pub_type_id
      """.stripMargin)
    type3_4.createOrReplaceTempView("type3_4")


    // 结题报告对应的所有有全文的成果
    val project_file_code = spark.sql(
      """
        |select * from (
        | select a.product_id,b.full_text_file_code,product_type from project_product a left join type3_4 b on a.id=b.id  where full_text_file_code is not null
        |)a group by product_id,full_text_file_code,product_type
      """.stripMargin)

    project_file_code.createOrReplaceTempView("project_file_code")


    //  一个product_id对应两个成果全文，但是product_id 对应的成果全文相同 (局部数据验证)
    //    27e3e624e67045cb981063741473660d_FS04    cbccd4b853a2400782c10eec65580353_FS04

    val project_full_text_pdf = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by product_id order by full_text_file_code desc ) rank from project_file_code
        | )a where rank =1
      """.stripMargin).drop("rank").cache()

    project_full_text_pdf.createOrReplaceTempView("project_full_text_pdf")

    // insert hive
    spark.sql(s"insert into ${fulltext_pdf_table}  select * from project_full_text_pdf")

    // download
    var num = 1
    project_full_text_pdf.collect().foreach(x=>{
      val savePath = pdfDir+x.get(0)+".pdf"
      println(num+"----------------->>>"+x.get(0)+".pdf------http://isisn.nsfc.gov.cn/egrantpdf/file/ajax-filedownload4Peer?fileCode="+x.get(1))
      val httpURL=isisnUrl+x.get(1)

      DownloadFromUrl.download(hdfsUrl,savePath,httpURL)
      num+=1
    })

  }




  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {

    final_year = MapUtil.get(map,"final_year").asInstanceOf[String]
    fulltext_pdf_table = MapUtil.get(map,"fulltext_pdf_table").asInstanceOf[String]
    hdfsUrl = MapUtil.get(map,"hdfsUrl").asInstanceOf[String]
    pdfDir = MapUtil.get(map,"pdfDir").asInstanceOf[String]
    isisnUrl = MapUtil.get(map,"isisnUrl").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {


    var descriptor : List[PropertyDescriptor] = List()

    val isisnUrl = new PropertyDescriptor().name("isisnUrl").displayName("isisnUrl").defaultValue("http://isisn.nsfc.gov.cn/egrantpdf/file/ajax-filedownload4Peer?fileCode=").description("isisnUrl").required(true)
    descriptor = isisnUrl :: descriptor

    val pdfDir = new PropertyDescriptor().name("pdfDir").displayName("pdfDir").defaultValue("/work/pdf/pdf_fulltext/").description("pdfDir").required(true)
    descriptor = pdfDir :: descriptor

    val hdfsUrl = new PropertyDescriptor().name("hdfsUrl").displayName("hdfsUrl").defaultValue("hdfs://192.168.3.138:8020").description("hdfsUrl").required(true)
    descriptor = hdfsUrl :: descriptor

    val fulltext_pdf_table = new PropertyDescriptor().name("fulltext_pdf_table").displayName("fulltext_pdf_table").defaultValue("middle_piflow.m_project_fulltext_pdf").description("fulltext_pdf_table").required(true)
    descriptor = fulltext_pdf_table :: descriptor

    val final_year = new PropertyDescriptor().name("final_year").displayName("final_year").defaultValue("2019").description("final_year").required(true)
    descriptor = final_year :: descriptor

    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/hive.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }


}