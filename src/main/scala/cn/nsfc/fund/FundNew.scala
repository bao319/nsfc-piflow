package cn.nsfc.fund

import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql._


class FundNew extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "FundNew"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var outputTable      :String= _
  var conference_table :String= _
  var journal_table    :String= _
  var award_table      :String= _
  var patent_table     :String= _
  var book_table       :String= _


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    println("start------------- FundNew ")

    val spark = pec.get[SparkSession]()

    //
    val product_rpt = spark.sql("select * from middle_piflow.m_product_business_fk_wash where product_type = '3' or product_type = '4' or product_type = '1' or product_type = '5' or product_type = '51' ")
    product_rpt.createOrReplaceTempView("product_rpt")

    //
    val prj_product = spark.sql(
      """
        |select a.id as rpt_id,prj_code,b.*  from (
        |   select a.prj_code,b.id from origin_piflow.o_project a
        |   left join origin_piflow.o_rpt_schedule b on a.prj_code = b.prj_code
        |   where  substr(a.end_date,0,4)>2016
        | )a left join product_rpt b  on a.id = b.key_code
        | where product_id is not null
      """.stripMargin)

    prj_product.createOrReplaceTempView("prj_product")

    // 1096216 -->   1108385
    println(spark.sql("select prj_code,product_id from prj_product group by prj_code,product_id").count())  //




    spark.sql(
      s"""
         |insert into ${outputTable} select prj_code,product_id from prj_product group by prj_code,product_id
      """.stripMargin)


    //    1010366

    val product_business = spark.sql("select * from middle_piflow.m_product_business_clean a where exists (select * from prj_product b where a.product_id=b.product_id)")

    product_business.createOrReplaceTempView("product_business")

    println("product_business ------ "+product_business.count())



    spark.sqlContext.udf.register("CleanTitle", (str: String) => {
      if (str == null)  str
      else {
        str.replaceAll("<.*?>","").trim
      }
    })


    println("-------------------------------------3")

    spark.sql(
      """
        |select
        |    a.product_id
        |   ,a.product_type
        |   ,a.type
        |   ,null as psn_code
        |   ,CleanTitle(a.zh_title) as zh_title
        |   ,CleanTitle(a.en_title) as en_title
        |   ,a.authors
        |   ,'product_business' as source
        |   ,split(a.publish_date,'-')[0] as publish_year
        |   ,split(a.publish_date,'-')[1]as publish_month
        |   ,split(a.publish_date,'-')[2] as publish_day
        |   ,a.list_info
        |   ,b.doi
        |   ,b.has_full_text
        |   ,b.paper_type_name  as   proceeding_name
        |   ,b.paper_type       as   proceeding_type
        |   ,null  as   language
        |   ,null  as   proceeding_address
        |   ,b.conf_org         as  proceeding_organizer
        |   ,b.conf_start_year  as  start_time
        |   ,b.conf_end_year    as  end_time
        |   ,b.page_range as  page_range
        |   ,null as  article_type
        |   ,b.country_name     as         country
        |   ,b.city             as         city
        |   ,b.article_no       as         article_no
        |   ,null as issn
        |   ,b.zh_abstract      as         zh_abstract
        |   ,b.zh_key_word      as         zh_keyword
        |   from (select * from product_business  where product_type = '3')a
        |   left join temp_piflow.t_product_business_ext_type3 b  on a.id =b.id
        |
        """.stripMargin).createOrReplaceTempView("product_business_conference")
    spark.sql(s"insert into ${conference_table}  select * from product_business_conference")








    println("-------------------------------------4")
    spark.sql(
      """
        |select
        |      a.product_id
        |      ,a.product_type
        |      ,a.type
        |      ,null as psn_code
        |      ,CleanTitle(a.zh_title) as zh_title
        |      ,CleanTitle(a.en_title) as en_title
        |      ,a.authors
        |      ,'product_business' as  source
        |      ,split(a.publish_date,'-')[0] as publish_year
        |      ,split(a.publish_date,'-')[1]as publish_month
        |      ,split(a.publish_date,'-')[2] as publish_day
        |      ,a.list_info
        |      ,b.doi
        |      ,b.has_full_text
        |      ,b.journal_name
        |      ,null as language
        |      ,b.public_status as status
        |      ,b.article_no  as  article_no
        |      ,null as  citedby_count
        |      ,b.volume as  volume
        |      ,null as  series
        |      ,b.page_range as  page_range
        |      ,null as issn
        |      ,b.zh_abstract
        |      ,b.zh_key_word as zh_keyword
        |   from (select * from product_business  where product_type = '4')a
        |   left join temp_piflow.t_product_business_ext_type4 b  on a.id =b.id
        |
        """.stripMargin).createOrReplaceTempView("product_business_journal")

    spark.sql(s"insert into ${journal_table}  select * from product_business_journal")





    println("-------------------------------------1")
    spark.sql(
      """
        |select
        |   a.product_id
        |   ,a.product_type
        |   ,a.type
        |   ,  null as psn_code
        |    ,CleanTitle(a.zh_title) as zh_title
        |   ,CleanTitle(a.en_title) as en_title
        |   ,a.authors
        |   ,'product_business' as source
        |   ,split(a.publish_date,'-')[0] as publish_year
        |   ,split(a.publish_date,'-')[1]as publish_month
        |   ,split(a.publish_date,'-')[2] as publish_day
        |   ,a.list_info
        |   ,null as  doi
        |   ,null as  has_full_text
        |   ,b.reward_type_name
        |   ,b.reward_rank
        |   ,b.issued_by
        |   ,b.reward_number
        |   from (select * from product_business  where product_type = '1')a
        |   left join temp_piflow.t_product_business_ext_type1 b  on a.id =b.id
        |
        """.stripMargin).createOrReplaceTempView("product_business_award")
    spark.sql(s"insert into ${award_table}  select * from product_business_award")




    println("-------------------------------------5")
    spark.sql(
      """
        |select
        |   a.product_id
        |   ,a.product_type
        |   ,a.type
        |   , null as psn_code
        |   ,CleanTitle(a.zh_title) as zh_title
        |   ,CleanTitle(a.en_title) as en_title
        |   ,a.authors
        |   ,'product_business' as source
        |   ,split(a.publish_date,'-')[0] as publish_year
        |   ,split(a.publish_date,'-')[1]as publish_month
        |   ,split(a.publish_date,'-')[2] as publish_day
        |   ,a.list_info
        |   ,null as  doi
        |   ,null as  has_full_text
        |   ,b.patent_no
        |   ,b.country
        |   ,a.authors as  patentee
        |   ,null as  ipc
        |   ,null as  cpc
        |   ,null as  applicant
        |   ,b.issuing_unit
        |   ,b.patent_type
        |   ,b.patent_status
        |   from (select * from product_business  where product_type = '5')a
        |   left join temp_piflow.t_product_business_ext_type5 b  on a.id =b.id
        |
        """.stripMargin).createOrReplaceTempView("product_business_patent")

    spark.sql(
      """
        |select
        |    product_id
        |   ,product_type
        |   ,type
        |   ,psn_code
        |   ,zh_title
        |   ,en_title
        |   ,authors
        |   ,source
        |   ,publish_year
        |   ,publish_month
        |   ,publish_day
        |   ,list_info
        |   ,doi
        |   ,has_full_text
        |   ,patent_no
        |   ,country
        |   ,patentee
        |   ,ipc
        |   ,cpc
        |   ,applicant
        |   ,if (issueUnit is null ,issuing_unit,issueUnit)  as issuing_unit
        |   ,patent_type
        |   ,patent_status
        |  from
        |   (select a.*,b.issueUnit from product_business_patent a left join middle.m_issueunit_comparison b on trim(a.issuing_unit) = trim(b.issueUnitBak))a
        |
      """.stripMargin).createOrReplaceTempView("product_business_patent_new")





    spark.sql(s"insert into ${patent_table}  select * from product_business_patent_new")





    println("-------------------------------------51")

    spark.sql(
      """
        |select
        |   a.product_id
        |   ,a.product_type
        |   ,a.type
        |   ,  null as psn_code
        |   ,CleanTitle(a.zh_title) as zh_title
        |   ,CleanTitle(a.en_title) as en_title
        |   ,a.authors
        |   ,'product_business' as source
        |   ,split(a.publish_date,'-')[0] as publish_year
        |   ,split(a.publish_date,'-')[1]as publish_month
        |   ,split(a.publish_date,'-')[2] as publish_day
        |   ,a.list_info
        |   ,null as  doi
        |   ,null as  has_full_text
        |   ,null as  book_name
        |   ,null as  book_series_name
        |   ,b.language
        |   ,b.status
        |   ,b.isbn
        |   ,a.authors as  editor
        |   ,b.country
        |   ,b.city
        |   ,null as  page_range
        |   ,b.word_count
        |   ,b.publisher
        |   from (select * from product_business  where product_type = '51')a
        |   left join temp_piflow.t_product_business_ext_type51 b  on a.id =b.id
        |
        """.stripMargin).createOrReplaceTempView("product_business_book")
    spark.sql(s"insert into  ${book_table}  select * from product_business_book")

    }


  def initialize(ctx: ProcessContext): Unit = {

  }


  def setProperties(map : Map[String, Any]): Unit = {
    outputTable = MapUtil.get(map,"outputTable").asInstanceOf[String]
    conference_table = MapUtil.get(map,"conference_table").asInstanceOf[String]
    journal_table = MapUtil.get(map,"journal_table").asInstanceOf[String]
    award_table = MapUtil.get(map,"award_table").asInstanceOf[String]
    patent_table = MapUtil.get(map,"patent_table").asInstanceOf[String]
    book_table = MapUtil.get(map,"book_table").asInstanceOf[String]

  }


  override def getPropertyDescriptor(): List[PropertyDescriptor] = {

    var descriptor : List[PropertyDescriptor] = List()
    val outputTable = new PropertyDescriptor().name("outputTable").displayName("outputTable").defaultValue("middle_piflow.m_fund_new").description("m_fund_new").required(true)
    descriptor = outputTable :: descriptor

    val conference_table = new PropertyDescriptor().name("conference_table").displayName("conference_table").defaultValue("middle_piflow.m_product_business_conference").description("conference_table").required(true)
    descriptor = conference_table :: descriptor

    val journal_table = new PropertyDescriptor().name("journal_table").displayName("journal_table").defaultValue("middle_piflow.m_product_business_journal").description("journal_table").required(true)
    descriptor = journal_table :: descriptor

    val award_table = new PropertyDescriptor().name("award_table").displayName("award_table").defaultValue("middle_piflow.m_product_business_award").description("award_table").required(true)
    descriptor = award_table :: descriptor

    val patent_table = new PropertyDescriptor().name("patent_table").displayName("patent_table").defaultValue("middle_piflow.m_product_business_patent").description("patent_table").required(true)
    descriptor = patent_table :: descriptor

    val book_table = new PropertyDescriptor().name("book_table").displayName("book_table").defaultValue("middle_piflow.m_product_business_book").description("book_table").required(true)
    descriptor = book_table :: descriptor


    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/hive.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }

}


