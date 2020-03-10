package cn.nsfc.personDistinct


import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql.{DataFrame, SparkSession}

class PersonDistinctBirthday extends ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "PersonDistinctBirthday"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var spark :SparkSession = null

  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    spark = pec.get[SparkSession]()
    distinct_birthday(spark)

  }

  def distinct_birthday(spark:SparkSession): Unit ={

    val m_person = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by psn_code order by psn_code ) rank from middle_piflow.m_person_org
        |)a where rank =1
      """.stripMargin).drop("rank")
    m_person.createOrReplaceTempView("m_person")

    val origin_person = spark.sql("select * from m_person where length(psn_code)>30").cache()
    origin_person.createOrReplaceTempView("origin_person")

    val org_exists = spark.sql("select * from origin_person where zh_name is not null and org_code is not null and birthday is not null").cache()
    org_exists.createOrReplaceTempView("org_exists")
    println(org_exists.count())

    val org_exists_dis = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by zh_name,org_code,substr(birthday,0,4) order by psn_code  ) rank from org_exists
        |)a where rank =1
      """.stripMargin).drop("rank").cache()


    org_exists_dis.createOrReplaceTempView("org_exists_dis")
    val old_new_org = spark.sql(
      """
        |select a.psn_code as old,b.psn_code as new,a.org_code from org_exists a left join org_exists_dis b  on a.zh_name= b.zh_name and a.org_code=b.org_code and substr(a.birthday,0,4)= substr(b.birthday,0,4)
      """.stripMargin)
    old_new_org.createOrReplaceTempView("old_new_org")
    println("old_new_org---"+old_new_org.count())

    val org_dis = origin_person.except(org_exists).union(org_exists_dis).cache()
    org_dis.createOrReplaceTempView("org_dis")
    println("org_dis------------"+org_dis.count())


    val birthday_exists: DataFrame = spark.sql("select * from org_dis where zh_name is not null and birthday is not null")
    birthday_exists.createOrReplaceTempView("birthday_exists")


    println("--------------------------------------birthday------------------------------")
    val birthday_exists_dis = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by zh_name,substr(birthday,0,10) order by psn_code  ) rank from birthday_exists
        |)a where rank =1
      """.stripMargin).drop("rank").cache()
    birthday_exists_dis.createOrReplaceTempView("birthday_exists_dis")


    // relation
    val old_new_bir = spark.sql(
      """
        |select a.psn_code as old,b.psn_code as new,a.org_code from birthday_exists a left join birthday_exists_dis b  on a.zh_name= b.zh_name and substr(a.birthday,0,10)= substr(b.birthday,0,10)
      """.stripMargin).cache()
    old_new_bir.createOrReplaceTempView("old_new_bir")

    val old_new_org_bir = spark.sql(
      """
        |select * from (
        |select old,if(new_b is null ,new ,new_b) as new ,org_code from (
        |select a.old,a.new,a.org_code ,b.new  as new_b from old_new_org a left join old_new_bir b on a.new  = b.old
        |)a   union select * from  old_new_bir )a group by old,new,org_code
        |
      """.stripMargin).cache()
    old_new_org_bir.createOrReplaceTempView("old_new_org_bir")
    println(old_new_org_bir.count())


    // person_dis
    val birthday_org_exists_dis = org_dis.except(birthday_exists).union(birthday_exists_dis).cache()
    birthday_org_exists_dis.createOrReplaceTempView("birthday_org_exists_dis")
    println("birthday_exists_dis----"+birthday_org_exists_dis.count())



    //------------------------------ compare with o_person -----------------------------------------------
    val o_person= spark.sql("select * from m_person where length(psn_code)<30").cache()
    o_person.createOrReplaceTempView("o_person")


    //  a.zh_name= b.zh_name and a.org_code=b.org_code and substr(a.birthday,0,4)= substr(b.birthday,0,4)
    val org_year_filter = spark.sql("select * from birthday_org_exists_dis where zh_name is not null and org_code is not null and birthday is not null")
    org_year_filter.createOrReplaceTempView("org_year_filter")


    val except_org_year = spark.sql(
      """
        |select  * from org_year_filter  a where not exists (select * from o_person b where a.zh_name= b.zh_name and a.org_code=b.org_code and substr(a.birthday,0,4)= substr(b.birthday,0,4))
      """.stripMargin).union(birthday_org_exists_dis.except(org_year_filter)).cache()
    except_org_year.createOrReplaceTempView("except_org_year")
    println("except_org_year   "+except_org_year.count())

    val rel_org_year_exists = spark.sql(
      """
        |select * from (
        |    select a.psn_code as  old,b.psn_code  from (
        |            select  * from org_year_filter  a where  exists (select * from o_person b where a.zh_name= b.zh_name and a.org_code=b.org_code and substr(a.birthday,0,4)= substr(b.birthday,0,4)))a
        |     left join o_person b on a.zh_name= b.zh_name and a.org_code=b.org_code and substr(a.birthday,0,4)= substr(b.birthday,0,4)
        | )a group by old,psn_code
        |
      """.stripMargin)
    rel_org_year_exists.createOrReplaceTempView("rel_org_year_exists")



    // a.zh_name= b.zh_name  and substr(a.birthday,0,10)= substr(b.birthday,0,10)
    val bir_filter = spark.sql("select * from except_org_year where zh_name is not null  and birthday is not null")
    bir_filter.createOrReplaceTempView("bir_filter")

    val except_birthday = spark.sql(
      """
        |select  * from bir_filter  a where not exists (select * from o_person b where a.zh_name= b.zh_name  and substr(a.birthday,0,10)= substr(b.birthday,0,10))
      """.stripMargin).union(except_org_year.except(bir_filter)).cache()

    println("except_birthday   "+except_birthday.count())

    val rel_bir_exists = spark.sql(
      """
        |select * from (
        |    select a.psn_code as  old,b.psn_code  from (
        |            select  * from bir_filter  a where  exists (select * from o_person b where a.zh_name= b.zh_name  and substr(a.birthday,0,10)= substr(b.birthday,0,10)))a
        |     left join o_person b on a.zh_name= b.zh_name  and substr(a.birthday,0,10)= substr(b.birthday,0,10)
        | )a group by old,psn_code
        |
      """.stripMargin)

    rel_bir_exists.createOrReplaceTempView("rel_bir_exists")

    val rel_exists = rel_org_year_exists.union(rel_bir_exists).cache()
    rel_exists.createOrReplaceTempView("rel_exists")




    // relation
    println("-----rel_old_new------")
    val rel_old_new =spark.sql(
      """
        |select * from (
        | select old,if(psn_code is null,new,psn_code) as new ,org_code  from (
        |   select a.old,a.new,a.org_code,b.psn_code  from old_new_org_bir a left join rel_exists b on a.new=b.old
        | )a
        | )a
        |group by old,new ,org_code
      """.stripMargin).cache()

    println(old_new_org_bir.count())
    println(rel_old_new.count())

    rel_old_new.createOrReplaceTempView("rel_old_new")

    rel_old_new.show()



    println("write to  middle_piflow.m_person_new")
    except_birthday.union(o_person).createOrReplaceTempView("m_person_new")

    spark.sql(
      """
        |select
        |    psn_code
        |    ,org_code
        |    ,org_name
        |    ,dept_code
        |    ,zh_name
        |    ,card_type
        |    ,card_code
        |    ,zero
        |    ,identity_card
        |    ,military_id
        |    ,passport
        |    ,four
        |    ,home_return_permit
        |    ,mainland_travel_permit_for_taiwan_residents
        |    ,mobile
        |    ,email
        |    ,status
        |    ,create_date
        |    ,title
        |    ,b.prof_title
        |    ,penable
        |    ,edited
        |    ,a.prof_title as prof_title_id
        |    ,update_time
        |    ,tel
        |    ,gender
        |    ,birthday
        |    ,ethnicity
        |    ,regioncode
        |    ,position
        |    ,degreecode
        |    ,degreeyear
        |    ,degreecountry
        |    ,major
        |    ,fax
        |    ,backupemail
        |    ,address
        |    ,postcode
        |    ,province
        |    ,city
        |    ,valid_email
        |    ,valid_mobile
        |    ,valid_card
        |    ,login_time
        |    ,scholarmate_url
        |    ,is_academician
        |    ,is_outstandingyouth
        |    ,source
        |    ,start_date
        |  from   m_person_new  a left join     middle.m_professional_title  b on a.prof_title=b.code
        |
      """.stripMargin).createOrReplaceTempView("m_person_new_prof_title")


    spark.sql(s"insert into middle_piflow.m_person_new select *  from m_person_new_prof_title")



    println("write to  middle_piflow.m_project_person_new")
    spark.sql(
      """
        |select * from (
        |  select id,prj_code , if(new is null,psn_code,new) as psn_code,org_code_new,org_name,prof_title,if(new is null,exactRule,exactRule_new) as exactRule  from (
        |     select a.*,b.new,b.org_code as org_code_new,'false' as exactRule_new from middle_piflow.m_project_person a left join rel_old_new b on a.psn_code = b.old
        |  )a
        |)a
        |
      """.stripMargin).createOrReplaceTempView("project_person_new")
    spark.sql(
      """
        |insert into middle_piflow.m_project_person_new
        |  select id ,prj_code,psn_code,org_code_new,org_name ,b.prof_title,exactRule from project_person_new  a
        |    left join    middle.m_professional_title  b on a.prof_title=b.code
        |
      """.stripMargin)


    println("write to  middle_piflow.m_proposal_person_new")
    spark.sql(
      """
        |select * from (
        |  select id,prp_code , if(new is null,psn_code,new) as psn_code,seq_no,org_code_new,org_name,prof_title,if(new is null,exactRule,exactRule_new) as exactRule   from (
        |     select a.*,b.new,b.org_code as org_code_new,'false' as exactRule_new from middle_piflow.m_proposal_person a left join rel_old_new b on a.psn_code = b.old
        |  )a
        |)a
        |
      """.stripMargin).createOrReplaceTempView("proposal_person_new")

    spark.sql(
      """
        |insert into middle_piflow.m_proposal_person_new
        |  select id ,prp_code,psn_code,seq_no,org_code_new,org_name ,b.prof_title,exactRule from proposal_person_new a
        |    left join    middle.m_professional_title  b on a.prof_title=b.code
        |
      """.stripMargin)


  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  def setProperties(map : Map[String, Any]): Unit = {
//    hiveQL = MapUtil.get(map,"hiveQL").asInstanceOf[String]
  }

  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()
//    val hiveQL = new PropertyDescriptor().name("hiveQL").displayName("HiveQL").defaultValue("").allowableValues(Set("")).required(true)
//    descriptor = hiveQL :: descriptor
    descriptor
  }

  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("png/hive.png")
  }

  override def getGroup(): List[String] = {
    List(StopGroup.NSFC.toString)
  }


}
