package cn.nsfc.productDistinct

import java.util.regex.Pattern

import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql.SparkSession

class ProductPersonDistinct extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "ProfuctPersonDistinct"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var table :String =_
  var outPutTable :String =_
  var rel_table :String =_
  var typeTable :String =_

  var spark :SparkSession = null


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

     spark = pec.get[SparkSession]()

    if (typeTable.equals("product_business")) {

      product_business_neo4j_5(spark, outPutTable)

    } else if (typeTable.equals("product_person")) {
      println("start----------------------------middle_piflow.m_product_person")

      distinct(spark, table, outPutTable, rel_table)

    }
  }



  def distinct(spark:SparkSession,table: String,outPutTable:String,rel_table:String): Unit = {

      val author_zh_en_isNULLString = s"select * from ${table} where zh_title is null and en_title is  null and authors is null"
      val originString = s"select * from ${table}"
      val zh_en_isNULLString =  s"select * from ${table} where zh_title is null and en_title is  null "

      val author_zh_en_isNULL = spark.sql(author_zh_en_isNULLString)
      val origin = spark.sql(originString).except(author_zh_en_isNULL).except(spark.sql(zh_en_isNULLString)).cache()
      origin.createOrReplaceTempView("origin")
      println(table+"------------------------->"+origin.count())

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


      // filter authors  zh_title
      spark.sql("select * from temp where authors is not null and zh_title is not null").createOrReplaceTempView("author_zh_isNOTNULL")
      val zh_Distinct = spark.sql(
        """
          |select * from (
          |select *, row_number() over (partition by clean_authors,clean_zh_title order by update_date desc,id desc ) rank from author_zh_isNOTNULL
          | )a where rank =1
        """.stripMargin).drop("rank")
      zh_Distinct.createOrReplaceTempView("zh_Distinct")

      val old_new_author_zh = spark.sql(
        """
          |select a.id as old,b.id as new from author_zh_isNOTNULL a left join zh_Distinct b  on a.clean_authors= b.clean_authors and a.clean_zh_title=b.clean_zh_title
        """.stripMargin)


      // authors is  null or zh_title is  null
      val author_zh_or_NULL = spark.sql("select * from temp where authors is  null or zh_title is  null")
        author_zh_or_NULL.createOrReplaceTempView("author_zh_or_NULL")

      // zh_title is  null
      val zh_isNULL = spark.sql(
            """
              |select * from author_zh_or_NULL where zh_title is  null
            """.stripMargin)
      //zh_title is  not  null
      spark.sql("select * from author_zh_or_NULL where zh_title is not null").createOrReplaceTempView("zh_isNOTNULL")

      val zh_isNOTNULL_Distinct = spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by clean_zh_title order by update_date desc,id desc ) rank from zh_isNOTNULL)a where  rank =1
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

      println("                  zh_Filter            "+zh_Filter.count())


      //  filter authors  en_title

      spark.sql("select * from zh_Filter where authors is not null and en_title is not null").createOrReplaceTempView("author_en_isNOTNULL")
      val en_Distinct = spark.sql(
        """
          |select * from (
          |select *, row_number() over (partition by clean_authors,clean_en_title order by update_date desc,id desc ) rank from author_en_isNOTNULL
          | )a where rank =1
        """.stripMargin).drop("rank")
      en_Distinct.createOrReplaceTempView("en_Distinct")

      val old_new_author_en = spark.sql(
        """
          |select a.id as old,b.id as new from author_en_isNOTNULL a left join en_Distinct b  on a.clean_authors= b.clean_authors and a.clean_en_title=b.clean_en_title
        """.stripMargin)


//    authors is  null or en_title is  null
      val author_en_or_NULL = spark.sql("select * from zh_Filter where authors is  null or en_title is  null")
      author_en_or_NULL.createOrReplaceTempView("author_en_or_NULL")

//      en_title is  null
      val en_isNULL = spark.sql(
        """
          |select * from author_en_or_NULL where en_title is  null
        """.stripMargin)
      spark.sql("select * from author_en_or_NULL where en_title is not null").createOrReplaceTempView("en_isNOTNULL")

      val en_isNOTNULL_Distinct = spark.sql(
        """
          |select * from (
          |select *, row_number() over (partition by clean_en_title order by update_date desc,id desc ) rank from en_isNOTNULL)a where rank =1
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
      println(  "           en_Filter        " + en_Filter.count())



      // replace  product_id
      val en_Filter_Fields: Array[String] = en_Filter.schema.fieldNames
      val product_sqlString = new StringBuilder
      for (i <- 0 until en_Filter_Fields.length) {
        if (en_Filter_Fields(i).equals("product_id")) product_sqlString.append("if(product_id_new is null,product_id,product_id_new) as product_id ,")
        else product_sqlString.append(en_Filter_Fields(i) + ",")
      }

      val productString = "select " + product_sqlString.toString.dropRight(1)
      val product_clean_replace = spark.sql(
        s"""
           |    ${productString}
           |    from (
           |      select a.* ,b.product_id as product_id_new from en_Filter  a left join middle_piflow.m_product_business_neo4j b
           |   on a.clean_zh_title = b.clean_zh_title and a.product_type =b.product_type)a
           |
         """.stripMargin).drop("clean_zh_title").drop("clean_authors").drop("clean_en_title").cache()

      println("         product_clean_replace             "+product_clean_replace.count())

      product_clean_replace.createOrReplaceTempView("product_person_clean_replace")



      // Mapping relation table  , product_person
      val product_person = spark.sql(
        """
          |  select a.id,a.psn_code,b.product_id from (
          |  select id,psn_code,b.new from temp a left join old_new b on a.id = b.old )a
          |  left join product_person_clean_replace b on a.new = b.id
          |
        """.stripMargin).cache()
      product_person.createOrReplaceTempView("product_person")

      val product_person_exists= spark.sql("select * from product_person a  where exists (select * from origin_piflow.o_person b where a.psn_code = b.psn_code)").cache()
      product_person_exists.createOrReplaceTempView("product_person_exists")

      val product_person_exists_distinct = spark.sql(
        """
          |select * from (
          |select *, row_number() over (partition by psn_code,product_id order by id desc ) rank from product_person_exists)a where rank =1
        """.stripMargin).drop("rank")
      product_person_exists_distinct.createOrReplaceTempView("product_person_exists_distinct")

      println( "           ------======>product_person ---" + "product_person.count()")
      println("product_person_exists.count()")
      println("product_person_exists_distinct.count()")
      spark.sql(s"insert into ${rel_table} select id,psn_code,product_id from product_person_exists_distinct")



      // cn.nsfc.distinct
      val product_person_clean_replace_not_exists = spark.sql(
        """
          |select * from product_person_clean_replace a  where
          | not  exists (select * from middle_piflow.m_product_business_neo4j b where a.product_id = b.product_id and a.product_type=b.product_type)
        """.stripMargin)
      println("product_person_clean_replace_not_exists         " + "product_person_clean_replace_not_exists.count()")
      product_person_clean_replace_not_exists.createOrReplaceTempView("product_person_clean_replace_not_exists")

      val product_distinct = spark.sql(
        """
          |select * from (
          |select *, row_number() over (partition by product_id order by update_date desc,id desc ) rank from product_person_clean_replace_not_exists)a where rank =1
        """.stripMargin).drop("rank")
      product_distinct.createOrReplaceTempView("product_distinct")
      println("               product_distinct   "+product_distinct.count())




      println("------------------------------------------------------")

      spark.sqlContext.udf.register("dropRightString", (str: String) => {
         str.stripSuffix(",")
      })


    //1
    if (table.contains("t_product_person_journal_complement")){
            val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")

          val frame = spark.sql(
            """
              |
              |select * ,dropRightString(concat(if(list_ei=1,'EI,',''),if(list_ei=1,'SCI,',''),if(list_ei=1,'SSCI,',''),if(list_ei=1,'ISTP',''))) as include_other,
              | if(zh_abstract is null ,summary,zh_abstract) as zh_abstract_new,if(zh_keyword is null ,keywords,zh_keyword) as zh_keyword_new
              |from (
              |   select a.*,summary ,keywords  from df a left join
              |    (select product_id,summary,keywords from origin_piflow.o_product_person_extend group by product_id,summary,keywords)b
              |    on a.product_id = b.product_id
              | )a
              |
            """.stripMargin).drop("zh_abstract").drop("zh_keyword").drop("summary").drop("keywords")
            .drop("list_ei").drop("list_sci").drop("list_ssci").drop("list_istp")



          frame.createOrReplaceTempView("frame")

          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from frame)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("journal")

          spark.sql(s"insert into ${outPutTable} select * from journal")

        }
        //2
        if(table.contains("t_product_person_conf_report_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")

          df.createOrReplaceTempView("df")
          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")


            spark.sql(s"insert into ${outPutTable} select * from frame")
        }
        //3
        if(table.contains("t_product_person_talent_training_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
          df.createOrReplaceTempView("df")
          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")
        }
        //4
        if(table.contains("t_product_person_proceeding_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
          df.createOrReplaceTempView("df")


          val frame = spark.sql(
            """
              |
              |select * ,dropRightString(concat(if(list_ei=1,'EI,',''),if(list_ei=1,'SCI,',''),if(list_ei=1,'SSCI,',''),if(list_ei=1,'ISTP',''))) as include_other,
              | if(zh_abstract is null ,summary,zh_abstract) as zh_abstract_new,if(zh_keyword is null ,keywords,zh_keyword) as zh_keyword_new
              |from (
              |   select a.*,summary ,keywords  from df a left join
              |    (select product_id,summary,keywords from origin_piflow.o_product_person_extend group by product_id,summary,keywords)b
              |    on a.product_id = b.product_id
              | )a
              |
            """.stripMargin).drop("zh_abstract").drop("zh_keyword").drop("summary").drop("keywords")
              .drop("list_ei").drop("list_sci").drop("list_ssci").drop("list_istp")
          frame.createOrReplaceTempView("proceeding")



          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from proceeding)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")

          spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //5
        if(table.contains("t_product_person_criterion_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")
          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")

            spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //6
        if(table.contains("t_product_person_academic_conference_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")
          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //7
        if(table.contains("t_product_person_academic_monograph_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")
          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")


            spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //8
        if(table.contains("t_product_person_software_copyright_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")

          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //9
        if(table.contains("t_product_person_technology_transfer_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")

          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //10
        if(table.contains("t_product_person_patent_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")

          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")

        }
        //11
        if(table.contains("t_product_person_scientific_reward_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")

          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")
        }
        //12
        if(table.contains("t_product_person_other_complement")){
          val df = spark.sql("select * from product_distinct").drop("clean_authors").drop("clean_zh_title").drop("clean_en_title")
            df.createOrReplaceTempView("df")

          spark.sql(
            """
              |select * from (
              |select *, row_number() over (partition by id order by update_date desc) rank from df)a where rank =1
            """.stripMargin).drop("rank").createOrReplaceTempView("frame")
            spark.sql(s"insert into ${outPutTable} select * from frame")
        }


    }


  def product_business_neo4j_5(spark:SparkSession,outputTable:String): Unit = {

    println("start -----------------------  middle_piflow.m_product_business_neo4j  ")


    spark.sqlContext.udf.register("CleanString", (authors: String) => {
      if (authors == null) null
      else authors.replaceAll("<strong>", "").replace("</strong>", "")
        .replaceAll("<b>", "").replaceAll("</b>", "")
        .replaceAll("#", "").replaceAll(";","")
        .replaceAll("\\(","").replaceAll("\\)","").replaceAll("\\*","")
        .replaceAll(" ","").replaceAll(",","")
        .replaceAll("，","").replaceAll("；","")
        .replaceAll("-","").replaceAll("\\.","")
        .replaceAll("\t","").replaceAll("\\s","")
        .toLowerCase
    })

    val product_person = spark.sql(
      """
        |select * from (
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from middle_piflow.m_product_business_conference
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from middle_piflow.m_product_business_journal
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from middle_piflow.m_product_business_award
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from middle_piflow.m_product_business_patent
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from middle_piflow.m_product_business_book
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from npd_fund.m_npd_award
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from npd_fund.m_npd_book
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from npd_fund.m_npd_conference
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from npd_fund.m_npd_journal
        |union
        |select product_id,product_type,zh_title,authors,CleanString(zh_title) as clean_zh_title,CleanString(authors) as clean_authors   from npd_fund.m_npd_patent
        |)a group by product_id,product_type,zh_title,authors,clean_zh_title,clean_authors
      """.stripMargin)

    product_person.createOrReplaceTempView("product_person")
    product_person.show(5)
    spark.sql(s"insert into  ${outputTable} select * from product_person")

  }


  def setProperties(map : Map[String, Any]): Unit = {
    table = MapUtil.get(map,"table").asInstanceOf[String]
    outPutTable = MapUtil.get(map,"outPutTable").asInstanceOf[String]
    rel_table = MapUtil.get(map,"rel_table").asInstanceOf[String]
    typeTable = MapUtil.get(map,"typeTable").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val table = new PropertyDescriptor().name("table").displayName("table").defaultValue("")
      .description(" if  typeTable is 'product_person' ,table is 12 type,example  'temp_piflow.t_product_person_journal_complement' ....").required(false)
    descriptor = table :: descriptor

    val outPutTable = new PropertyDescriptor().name("outPutTable").displayName("outPutTable").defaultValue("middle_piflow.m_product_journal")
      .description("if typeTable is 'product_business' ,outPutTable is 'middle_piflow.m_product_business_neo4j';\n else typeTable is 'product_person' ,outPutTable is 12 type,example  'middle_piflow.m_product_journal' ....").required(true)
    descriptor = outPutTable :: descriptor

    val rel_table = new PropertyDescriptor().name("rel_table").displayName("rel_table").defaultValue("middle_piflow.m_product_person")
      .description("if typeTable is 'product_person' ,rel_table is 'middle_piflow.m_product_person'").required(false)
    descriptor = rel_table :: descriptor

    val typeTable = new PropertyDescriptor().name("typeTable").displayName("typeTable").defaultValue("product_person").allowableValues(Set("product_business","product_person")).required(true)
    descriptor = typeTable :: descriptor


    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/hive.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }

  override def initialize(ctx: ProcessContext): Unit = {

  }
}
