package cn.nsfc.outputPdf

import java.io._

import cn.nsfc.util.DownloadFromUrl
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql._



class ReportPdfDownload extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "ReportPdfDownload"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)


  var final_year:String = _
  var report_pdf_table:String = _
  var hdfsUrl:String = _
  var pdfDir:String = _
  var isisnUrl:String = _

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    println("start------------------------------ReportPdfDownload")
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

    project_pdf.show()
    // insert hive
    spark.sql(s"insert into ${report_pdf_table} select * from project_pdf")


    // download
    var num = 1
    project_pdf.collect().foreach(x=>{
      val savePath = pdfDir+x.get(1)+".pdf"
      println(num+"----------------->>>"+x.get(1)+".pdf------http://isisn.nsfc.gov.cn/egrantpdf/file/ajax-filedownload4Peer?fileCode="+x.get(3))
      val httpURL=isisnUrl+x.get(3)
      DownloadFromUrl.download(hdfsUrl,savePath,httpURL)
      num+=1
    })

  }




  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {

    final_year = MapUtil.get(map,"final_year").asInstanceOf[String]
    report_pdf_table = MapUtil.get(map,"report_pdf_table").asInstanceOf[String]
    hdfsUrl = MapUtil.get(map,"hdfsUrl").asInstanceOf[String]
    pdfDir = MapUtil.get(map,"pdfDir").asInstanceOf[String]
    isisnUrl = MapUtil.get(map,"isisnUrl").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {


    //    val final_year = "2019"
    //    val report_pdf_table="middle.m_project_rpt_pdf"
    //    val hdfsUrl = "hdfs://192.168.3.138:8020"
    //    val pdfDir = "/work/pdf/pdf_report/"
    //    val isisnUrl ="http://isisn.nsfc.gov.cn/egrantpdf/file/ajax-filedownload4Peer?fileCode="

    var descriptor : List[PropertyDescriptor] = List()

    val isisnUrl = new PropertyDescriptor().name("isisnUrl").displayName("isisnUrl").defaultValue("http://isisn.nsfc.gov.cn/egrantpdf/file/ajax-filedownload4Peer?fileCode=").description("isisnUrl").required(true)
    descriptor = isisnUrl :: descriptor

    val pdfDir = new PropertyDescriptor().name("pdfDir").displayName("pdfDir").defaultValue("/work/pdf/pdf_report/").description("pdfDir").required(true)
    descriptor = pdfDir :: descriptor

    val hdfsUrl = new PropertyDescriptor().name("hdfsUrl").displayName("hdfsUrl").defaultValue("hdfs://192.168.3.138:8020").description("hdfsUrl").required(true)
    descriptor = hdfsUrl :: descriptor

    val report_pdf_table = new PropertyDescriptor().name("report_pdf_table").displayName("report_pdf_table").defaultValue("middle_piflow.m_project_rpt_pdf").description("report_pdf_table").required(true)
    descriptor = report_pdf_table :: descriptor

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