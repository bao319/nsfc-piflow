package cn.nsfc.ubpaywall

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql._


class Ubpaywall extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "Ubpaywall"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var outputTable      :String= _
  var unpaywall_table :String= _
  var unpaywall2_onetitle_table    :String= _

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    println("start------------- Ubpaywall ")

    val spark = pec.get[SparkSession]()


    spark.sql(
      s"""
        |insert into  ${outputTable}
        |    select * from
        |    (select businessext_id, businessext_zh_title, businessext_en_title, business_product_id, businessext_authors_name, businessext_journal_name, business_publish_date, business_year, project_prj_no, organization_name, project_grant_name, businessext_pub_id, product_type, project_prj_code, project_zh_title, organization_org_code, project_grant_code, project_subject_code1, has_full_text, a.doi as doi, zh_abstract, zh_key_word,source,b.doi_url as doi_url,'ir'
        |    from middle.m_product_business_fulltext as a left join ${unpaywall_table} as b
        |    on lower(trim(a.doi)) = lower(trim(b.doi))) as t
        |      where doi is not null and trim(doi) <> ''
      """.stripMargin)


    spark.sql(
      s"""
        |insert into  ${outputTable}
        |    select businessext_id, businessext_zh_title, businessext_en_title, business_product_id, businessext_authors_name, businessext_journal_name, business_publish_date, business_year, project_prj_no, organization_name, project_grant_name, businessext_pub_id, product_type, project_prj_code, project_zh_title, organization_org_code, project_grant_code, project_subject_code1, has_full_text, b.doi as doi, zh_abstract, zh_key_word,source,b.doi_url as doi_url,'ir'
        |    from (select * from middle.m_product_business_fulltext where (doi is null or trim(doi) = '')) as a left join ${unpaywall2_onetitle_table} as b
        |    on lower(regexp_replace(a.businessext_zh_title, '\\s+', '')) = b.title
      """.stripMargin)



    }


  def initialize(ctx: ProcessContext): Unit = {

  }


  def setProperties(map : Map[String, Any]): Unit = {
    outputTable = MapUtil.get(map,"outputTable").asInstanceOf[String]
    unpaywall_table = MapUtil.get(map,"unpaywall_table").asInstanceOf[String]
    unpaywall2_onetitle_table = MapUtil.get(map,"unpaywall2_onetitle_table").asInstanceOf[String]

  }


  override def getPropertyDescriptor(): List[PropertyDescriptor] = {

    var descriptor : List[PropertyDescriptor] = List()
    val outputTable = new PropertyDescriptor().name("outputTable").displayName("outputTable").defaultValue("middle_piflow.m_product_business_fulltext_doi_url").description("m_fund_new").required(true)
    descriptor = outputTable :: descriptor

    val unpaywall_table = new PropertyDescriptor().name("unpaywall_table").displayName("unpaywall_table").defaultValue("middle_piflow.m_unpaywall").description("unpaywall_table").required(true)
    descriptor = unpaywall_table :: descriptor

    val unpaywall2_onetitle_table = new PropertyDescriptor().name("unpaywall2_onetitle_table").displayName("unpaywall2_onetitle_table").defaultValue("middle.m_unpaywall2_onetitle").description("unpaywall2_onetitle_table").required(true)
    descriptor = unpaywall2_onetitle_table :: descriptor



    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/hive.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }

}


