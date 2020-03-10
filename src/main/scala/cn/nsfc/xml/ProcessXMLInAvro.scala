package cn.nsfc.xml

import cn.nsfc.util.parseJsonPubExtend
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import org.apache.spark.sql.SparkSession

class ProcessXMLInAvro extends ConfigurableStop{
  override val authorEmail: String = "ygang@cnic.cn"
  override val description: String = "ProcessXMLInAvro"
  override val inportList: List[String] =List(PortEnum.DefaultPort.toString)
  override val outportList: List[String] = List(PortEnum.DefaultPort.toString)


  var relevance :String = _
  var labeled :String = _
  var language:String=_
  var pub_extendFields :String=_
  var product_xml_name :String="PRODUCT_XML"
  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val spark = pec.get[SparkSession]()

    val df = in.read()


    spark.sqlContext.udf.register("parseAuthors",(str:String)=>{
      parseJsonPubExtend.parseAuthor_piflow(str)
    })

    spark.sqlContext.udf.register("parseExtend",(str:String,pub_type_id:String)=>{
      parseJsonPubExtend.pub_extend(str,pub_type_id)
    })


    df.createOrReplaceTempView("test")

    if(relevance.equals("true")){
      relevance=s",${product_xml_name}.product.pub_basic.relevance"
    } else {
      relevance=""
    }
    if(labeled.equals("true")){
      labeled=s",${product_xml_name}.product.pub_basic.labeled"
    } else {
      labeled=""
    }
    if(language.equals("true")){
      language=s",${product_xml_name}.product.pub_basic.language"
    } else {
      language=""
    }




    val frame = spark.sql(
      s"""
         |select
         |id
         |,${product_xml_name}.product.pub_basic.authenticated
         |
         |    ,split(parseAuthors(${product_xml_name}.product.pub_basic.authors.author),'≌')[0] as psn_name
         |    ,split(parseAuthors(${product_xml_name}.product.pub_basic.authors.author),'≌')[1] as org_name
         |    ,split(parseAuthors(${product_xml_name}.product.pub_basic.authors.author),'≌')[2] as email
         |    ,split(parseAuthors(${product_xml_name}.product.pub_basic.authors.author),'≌')[3] as is_message
         |    ,split(parseAuthors(${product_xml_name}.product.pub_basic.authors.author),'≌')[4] as first_author
         |    ,split(parseAuthors(${product_xml_name}.product.pub_basic.authors.author),'≌ ')[5] as is_mine
         |
         |,${product_xml_name}.product.pub_basic.authors_name
         |,${product_xml_name}.product.pub_basic.cited_times
         |,${product_xml_name}.product.pub_basic.create_date
         |,${product_xml_name}.product.pub_basic.en_pub_type_name
         |,${product_xml_name}.product.pub_basic.en_source
         |,${product_xml_name}.product.pub_basic.en_title
         |,${product_xml_name}.product.pub_basic.full_link
         |,${product_xml_name}.product.pub_basic.full_text.description
         |,${product_xml_name}.product.pub_basic.full_text.file_code
         |,${product_xml_name}.product.pub_basic.full_text.file_name
         |,${product_xml_name}.product.pub_basic.full_text.upload_date
         |,${product_xml_name}.product.pub_basic.full_text_img_url
         |,${product_xml_name}.product.pub_basic.has_full_text
         |${labeled}
         |${language}
         |,${product_xml_name}.product.pub_basic.list_bdzw
         |,${product_xml_name}.product.pub_basic.list_cssci
         |,${product_xml_name}.product.pub_basic.list_ei
         |,${product_xml_name}.product.pub_basic.list_ei_source
         |,${product_xml_name}.product.pub_basic.list_istp
         |,${product_xml_name}.product.pub_basic.list_istp_source
         |,${product_xml_name}.product.pub_basic.list_qt
         |,${product_xml_name}.product.pub_basic.list_sci
         |,${product_xml_name}.product.pub_basic.list_sci_source
         |,${product_xml_name}.product.pub_basic.list_ssci
         |,${product_xml_name}.product.pub_basic.list_ssci_source
         |,${product_xml_name}.product.pub_basic.owner
         |,${product_xml_name}.product.pub_basic.product_mark
         |,${product_xml_name}.product.pub_basic.pub_date_desc
         |,${product_xml_name}.product.pub_basic.pub_detail_param
         |,${product_xml_name}.product.pub_basic.pub_id
         |,${product_xml_name}.product.pub_basic.pub_type_id
         |,${product_xml_name}.product.pub_basic.pub_update_date
         |,${product_xml_name}.product.pub_basic.public_date
         |,${product_xml_name}.product.pub_basic.public_day
         |,${product_xml_name}.product.pub_basic.public_month
         |,${product_xml_name}.product.pub_basic.public_year
         |,${product_xml_name}.product.pub_basic.publish_day
         |,${product_xml_name}.product.pub_basic.publish_month
         |,${product_xml_name}.product.pub_basic.publish_year
         |${relevance}
         |,${product_xml_name}.product.pub_basic.remark
         |,${product_xml_name}.product.pub_basic.status
         |,${product_xml_name}.product.pub_basic.update_mark
         |,${product_xml_name}.product.pub_basic.version_no
         |,${product_xml_name}.product.pub_basic.zh_abstract
         |,${product_xml_name}.product.pub_basic.zh_key_word
         |,${product_xml_name}.product.pub_basic.zh_pub_type_name
         |,${product_xml_name}.product.pub_basic.zh_source
         |,${product_xml_name}.product.pub_basic.zh_title
         |
         |,split(parseExtend(${product_xml_name}.product.pub_extend,product_xml.product.pub_basic.pub_type_id),'≌')[0] as pub_extend
         |,split(parseExtend(${product_xml_name}.product.pub_extend,product_xml.product.pub_basic.pub_type_id),'≌')[1] as issue_no1
         |,split(parseExtend(${product_xml_name}.product.pub_extend,product_xml.product.pub_basic.pub_type_id),'≌')[2] as issue_no2
         | from test
      """.stripMargin)


    spark.sqlContext.udf.register("pubExtend",(id:String,str:String,field:String)=>{
      parseJsonPubExtend.parsePub_extend(id,str,field)
    })

    val build = new StringBuilder
    pub_extendFields.split(",").foreach(x=>{
      build.append(" pubExtend(id,pub_extend,'"+x+"') as " + x+",")
    })

    frame.createOrReplaceTempView("frame")
    val outDF = spark.sql(
      s"""
         |select  *
         |, ${build.toString().stripSuffix(",")}
         | from frame
      """.stripMargin)


    outDF.printSchema()
    out.write(outDF)


  }



  override def setProperties(map: Map[String, Any]): Unit = {
    relevance = MapUtil.get(map,"relevance").asInstanceOf[String]
    labeled = MapUtil.get(map,"labeled").asInstanceOf[String]
    language = MapUtil.get(map,"language").asInstanceOf[String]
    pub_extendFields = MapUtil.get(map,"pub_extendFields").asInstanceOf[String]
//    product_xml_name = MapUtil.get(map,"product_xml_name").asInstanceOf[String]


  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val relevance = new PropertyDescriptor().name("relevance").displayName("relevance").description("Does the schema contain an 'relevance' field").
      allowableValues(Set("true","false")).defaultValue("true").required(true)
    descriptor = relevance :: descriptor

    val labeled = new PropertyDescriptor().name("labeled").displayName("labeled").description("Does the schema contain an 'labeled' field").
      allowableValues(Set("true","false")).defaultValue("true").required(true)
    descriptor = labeled :: descriptor

    val language = new PropertyDescriptor().name("language").displayName("language").description("Does the schema contain an 'language' field").
      allowableValues(Set("true","false")).defaultValue("false").required(true)
    descriptor = language :: descriptor

//    val product_xml_name = new PropertyDescriptor().name("product_xml_name").displayName("product_xml_name").description("Does the schema contain an 'language' field").
//      allowableValues(Set("PRODUCT_XML","product_xml")).defaultValue("product_xml").required(true)
//    descriptor = product_xml_name :: descriptor

    val pub_extendFields = new PropertyDescriptor().name("pub_extendFields").displayName("pub_extendFields").description("This pub tag contains fields").
      defaultValue("").required(true)
    descriptor = pub_extendFields :: descriptor

    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/XmlParser.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }


  override def initialize(ctx: ProcessContext): Unit = {

  }

}
