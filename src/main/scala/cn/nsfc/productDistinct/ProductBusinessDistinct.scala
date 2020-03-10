package cn.nsfc.productDistinct

import java.util.regex.Pattern

import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql._


class ProductBusinessDistinct extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "ProductBusinessDistinct"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var originTable:String= _
  var outputTable:String= _
  var rel_table  :String= _

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    println("please start------------- ProductBusinessDistinct ")
    val spark = pec.get[SparkSession]()

    println(originTable)
    val author_zh_en_isNULLString = s"select * from ${originTable} where zh_title is null and en_title is  null and authors is null"
    val originString = s"select * from ${originTable}"
    val  zh_en_isNULLString = s"select * from ${originTable} where zh_title is null and en_title is  null "

    val author_zh_en_isNULL = spark.sql(author_zh_en_isNULLString)
    spark.sql(originString).except(author_zh_en_isNULL).except(spark.sql(zh_en_isNULLString)).createOrReplaceTempView("product_business")

    val origin = spark.sql("select * from product_business where type = 'rptCompletion'").cache()
    println(origin.count())

    origin.createOrReplaceTempView("origin")

    // clean authors
    spark.sqlContext.udf.register("cleanAuthors", (authors: String) => {
      if (authors == null) null
      else authors.replaceAll("<strong>", "").replace("</strong>", "")
        .replaceAll("<b>", "").replaceAll("</b>", "")
        .replaceAll("#", "").replaceAll("\\*","")
        .replaceAll("\\(","").replaceAll("\\)","")
    })

    spark.sqlContext.udf.register("CleanString", (str: String) => {
      if (str == null) null
      else {

        val str1=  str.replaceAll("<strong>", "").replace("</strong>", "")
          .replaceAll("<b>", "").replaceAll("</b>", "")
          .replaceAll("#", "").replaceAll(";", "")
          .replaceAll("\\(", "").replaceAll("\\)", "").replaceAll("\\*", "")
          .replaceAll(" ", "").replaceAll(",", "")
          .replaceAll("，", "").replaceAll("；", "")
          .replaceAll("-", "").replaceAll("\\.", "")
          .replaceAll("\t", "").replaceAll("\\s", "")
          .toLowerCase

        val p = Pattern.compile("[^a-zA-Z0-9\u4E00-\u9FFF]")
        val matcher = p.matcher(str1)
        // 把其他字符替换成
        val str_new = matcher.replaceAll("")
        str_new

      }
    })


    val fieldNames: Array[String] = origin.schema.fieldNames
    val sqlString = new StringBuilder
    for (i <- 0 until fieldNames.length) {
      if (fieldNames(i).equals("authors")) sqlString.append("cleanAuthors(" + fieldNames(i) + ") as authors,")
      else sqlString.append(fieldNames(i) + ",")
    }
    spark.sql("select " + sqlString.toString() + " CleanString(authors) as clean_authors, CleanString(zh_title) as clean_zh_title," +
      "CleanString(en_title) as clean_en_title from origin").cache()
      .createOrReplaceTempView("temp")

    println("temp -------"+spark.sql("select * from temp").count())


    // filter authors  zh_title
    spark.sql("select * from temp where authors is not null and zh_title is not null").createOrReplaceTempView("author_zh_isNOTNULL")
    val zh_Distinct = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by clean_authors,clean_zh_title order by type,create_date desc,id desc ) rank from author_zh_isNOTNULL
        | )a where rank =1
      """.stripMargin).drop("rank")
    zh_Distinct.createOrReplaceTempView("zh_Distinct")
    val old_new_author_zh = spark.sql(
      """
        |select a.id as old,b.id as new from author_zh_isNOTNULL a left join zh_Distinct b  on a.clean_authors= b.clean_authors and a.clean_zh_title=b.clean_zh_title
      """.stripMargin)



    val author_zh_or_NULL = spark.sql("select * from temp where authors is  null or zh_title is  null")
    author_zh_or_NULL.createOrReplaceTempView("author_zh_or_NULL")

    val zh_isNULL = spark.sql(
      """
        |select * from author_zh_or_NULL where zh_title is  null
      """.stripMargin)

    //zh_title is  not  null
    spark.sql("select * from author_zh_or_NULL where zh_title is not null").createOrReplaceTempView("zh_isNOTNULL")
    val zh_isNOTNULL_Distinct = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by clean_zh_title order by type,create_date desc,id desc ) rank from zh_isNOTNULL)a where  rank =1
      """.stripMargin).drop("rank")
    zh_isNOTNULL_Distinct.createOrReplaceTempView("zh_isNOTNULL_Distinct")

    val old_new_zh = spark.sql(
      """
        |select a.id as old,b.id as new from zh_isNOTNULL a left join zh_isNOTNULL_Distinct b  on a.clean_zh_title=b.clean_zh_title
      """.stripMargin)



    val old_new_zh_Filter = old_new_zh.union(old_new_author_zh)
    old_new_zh_Filter.createOrReplaceTempView("old_new_zh_Filter")

    val zh_Filter = zh_Distinct.union(zh_isNOTNULL_Distinct).union(zh_isNULL)
    zh_Filter.createOrReplaceTempView("zh_Filter")

    println(" zh_Filter            "+zh_Filter.count())



    //  filter authors  en_title

    spark.sql("select * from zh_Filter where authors is not null and en_title is not null").createOrReplaceTempView("author_en_isNOTNULL")
    val en_Distinct = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by clean_authors,clean_en_title order by type,create_date desc,id desc ) rank from author_en_isNOTNULL
        | )a where rank =1
      """.stripMargin).drop("rank")
    en_Distinct.createOrReplaceTempView("en_Distinct")

    val old_new_author_en = spark.sql(
      """
        |select a.id as old,b.id as new from author_en_isNOTNULL a left join en_Distinct b  on a.clean_authors= b.clean_authors and a.clean_en_title=b.clean_en_title
      """.stripMargin)


    val author_en_or_NULL = spark.sql("select * from zh_Filter where authors is  null or en_title is  null")
    author_en_or_NULL.createOrReplaceTempView("author_en_or_NULL")

    val en_isNULL = spark.sql(
      """
        |select * from author_en_or_NULL where en_title is  null
      """.stripMargin)
    spark.sql("select * from author_en_or_NULL where en_title is not null").createOrReplaceTempView("en_isNOTNULL")


    val en_isNOTNULL_Distinct = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by clean_en_title order by type,create_date desc,id desc ) rank from en_isNOTNULL)a where rank =1
      """.stripMargin).drop("rank")
    en_isNOTNULL_Distinct.createOrReplaceTempView("en_isNOTNULL_Distinct")

    val old_new_en = spark.sql(
      """
        |select a.id as old,b.id as new from en_isNOTNULL a left join en_isNOTNULL_Distinct b  on a.clean_en_title=b.clean_en_title
      """.stripMargin)
    old_new_en.createOrReplaceTempView("old_new_en")


    val old_new_en_Filter = old_new_en.union(old_new_author_en)
    old_new_en_Filter.createOrReplaceTempView("old_new_en_Filter")

    old_new_zh_Filter.union(old_new_en_Filter)

    val old_new = spark.sql(
      """
        |select * from (
        |  select old,if(new_b is null ,new ,new_b) as new  from (
        |     select a.old ,a.new ,b.new as new_b from old_new_zh_Filter a left join old_new_en_Filter b on a.new = b.old
        |  )a  union select * from  old_new_en_Filter )a group by old,new
      """.stripMargin).cache()
    old_new.createOrReplaceTempView("old_new")


    val en_Filter = en_Distinct.union(en_isNOTNULL_Distinct).union(en_isNULL).cache()
    en_Filter.createOrReplaceTempView("en_Filter")

    println("en_Filter             "+en_Filter.count())

    println("old_new              +"+old_new.count())

    //13
    if(originTable.contains("m_product_business")){

      // 2794089
      val origin_Distinct = spark.sql(
        """
          |select * from (
          |select *, row_number() over (partition by product_id order by type ,id desc ) rank from en_Filter)a where rank =1
        """.stripMargin).drop("rank").drop("clean_zh_title").drop("clean_authors").drop("clean_en_title")
      origin_Distinct.createOrReplaceTempView("origin_Distinct")

      spark.sql(s"insert into ${outputTable} select * from origin_Distinct")

      println("origin_Distinct----------"+origin_Distinct.count())


      // Mapping relation table  , product_person
      val t_product_rpt = spark.sql(
        """
          |  select a.id,a.key_code,b.product_id,b.product_type from (
          |  select id,key_code,b.new from temp a left join old_new b on a.id = b.old )a
          |  left join en_Filter b on a.new = b.id
          |
        """.stripMargin).cache()
      t_product_rpt.createOrReplaceTempView("t_product_rpt")

      // Mapping relation table  , product_person
      println(originTable + "======>product_rpt ---" + t_product_rpt.count())

      //        t_product_rpt.show()
      spark.sql(s"insert into ${rel_table} select id,key_code,product_id,product_type from t_product_rpt")


    }
    //14
    if(originTable.contains("m_product_business_prp")){
      en_Filter.drop("clean_authors").drop("clean_zh_title").drop("clean_en_title").createOrReplaceTempView("df_business_prp")

      spark.sql(s"insert into ${outputTable} select * from df_business_prp")
    }


  }



  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {
    originTable = MapUtil.get(map,"originTable").asInstanceOf[String]
    outputTable = MapUtil.get(map,"outputTable").asInstanceOf[String]
    rel_table = MapUtil.get(map,"rel_table").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val originTable = new PropertyDescriptor().name("originTable").displayName("originTable").defaultValue("middle_piflow.m_product_business").description("originTable").required(true)
    descriptor = originTable :: descriptor

    val outputTable = new PropertyDescriptor().name("outputTable").displayName("outputTable").defaultValue("middle_piflow.m_product_business_clean").description("outputTable").required(true)
    descriptor = outputTable :: descriptor

    val rel_table = new PropertyDescriptor().name("rel_table").displayName("rel_table").defaultValue("middle_piflow.m_product_business_fk_wash").description("rel_table").required(true)
    descriptor = rel_table :: descriptor


    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/hive.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }

}
