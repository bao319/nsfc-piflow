package cn.nsfc.personDistinct

import java.text.{ParseException, SimpleDateFormat}
import java.util.UUID
import java.util.regex.Pattern

import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, PortEnum, StopGroup}
import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import org.apache.spark.sql._


class PersonDistinct extends  ConfigurableStop {

  val authorEmail: String = "ygang@cnic.cn"
  val description: String = "PersonDistinct"
  val inportList: List[String] = List(PortEnum.DefaultPort.toString)
  val outportList: List[String] = List(PortEnum.DefaultPort.toString)

  var spark :SparkSession = null


  override def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {
    spark = pec.get[SparkSession]()

    // Generate unique value
    spark.sqlContext.udf.register("uuidKey",(prp_code:String,psn_name:String,email:String,card_code:String,mobile:String,prof_title:String,tel:String,birthday:String,source:String )=>{
      md5(prp_code,psn_name,email,card_code,mobile,prof_title,tel,birthday,source)
    })

    spark.sqlContext.udf.register("CleanString", (str: String) => {
      if (str == null) null
      else {
        val str1=  str.replaceAll(" ", "")
          .replaceAll("-", "")
          .replaceAll("\t", "").replaceAll("\\s", "").trim
          .toLowerCase

        str1
      }
    })

    spark.sqlContext.udf.register("CleanCard", (originCard: String) => {
      if (originCard == null) null
      else if(originCard.length == 15 &&  Pattern.compile("[0-9]*").matcher(originCard).matches() && cityCode.contains(originCard.substring(0,2))) {
        val m_card = (new StringBuilder(originCard)).insert(6, "19").toString()
        val array: Array[Int] = Array(7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2)
        val strings: Array[String] = m_card.split("")
        var num = 0
        for (i <- 0 until strings.size) {
          num += strings(i).toInt * array(i)
        }
        var card_code: String = null
        val arrayModel: Array[String] = Array("1", "0", "x", "9", "8", "7", "6", "5", "4", "3", "2")
        card_code = m_card + arrayModel(num % 11)
        if(checkDate(card_code.toString)){
          card_code
        } else {
          originCard
        }
      } else {
        originCard
      }
    })


    // Start with  the  distinct  pj_member
    distinct_pj_member()
    // end with  stat_prp_person
    distinct_Prp()

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



  // Check id card
  val cityCode = Array("11", "12", "13", "14", "15", "21", "22", "23", "31", "32", "33", "34", "35", "36", "37",
    "41", "42", "43", "44", "45", "46", "50", "51", "52", "53", "54", "61", "62", "63", "64", "65", "71", "81", "82","90")
  def checkDate(idCard:String): Boolean ={
    val dateStr = idCard.substring(6, 10) + "-" + idCard.substring(10, 12) + "-" + idCard.substring(12, 14)
    val df = new SimpleDateFormat("yyyy-MM-dd")
    df.setLenient(false)
    try {
      val date = df.parse(dateStr)
      return date != null
    } catch {
      case e: ParseException =>
        // TODO Auto-generated catch block
        return false
    }

  }


  def distinct_pj_member(): Unit = {
    println("pj_member")
    val o_pj_member = spark.sql(
      """
        |select * from  (
        |       select
        |           a.prj_code
        |          ,a.zh_name  as psn_name
        |          ,a.org_name
        |          ,a.id_type   as card_type
        |          ,CleanCard(a.id_no)   as card_code
        |          ,a.birthday
        |          ,a.gender
        |          ,a.prof_title
        |          ,a.tel
        |          ,a.email
        |          ,a.org_code
        |          ,a.seq_no
        |          ,split(b.start_date,' ')[0] as start_date
        |          ,if(1=1,'o_pj_member','o_pj_member') as source
        |       from origin.o_pj_member a
        |       left join  origin.o_project b
        |       on a.prj_code=b.prj_code)a
        | group by
        |  prj_code
        | ,psn_name
        | ,org_name
        | ,card_type
        | ,card_code
        | ,birthday
        | ,gender
        | ,prof_title
        | ,tel
        | ,email
        | ,org_code
        | ,seq_no
        | ,start_date
        | ,source
      """.stripMargin).cache()
    println(o_pj_member.count())

    distinct("o_pj_member",o_pj_member,"tel")
  }

  def distinct_Prp(): Unit = {

    val o_stat_prp_person = spark.sql(
      """

        |select * from
        |      (select
        |          a.prp_code
        |          ,a.psn_name
        |          ,a.email
        |          ,a.org_name
        |          ,a.card_type
        |          ,CleanCard(a.card_code) as card_code
        |          ,a.mobile
        |          ,a.prof_title
        |          ,a.tel
        |          ,a.birthday
        |          ,a.gender
        |          ,a.seq_no
        |          ,split(b.start_date,' ')[0] as start_date
        |          ,if(1=1,'o_stat_prp_person','o_stat_prp_person') as source
        |      from origin.o_stat_prp_persons a
        |      left join  origin.o_stat_proposal b
        |      on a.prp_code= b.prp_code
        |      where a.psn_name is  not null
        |    )c
        |  group  by
        |  prp_code,psn_name,email,org_name,card_type,card_code
        | ,mobile,prof_title,tel,birthday,gender,seq_no,start_date,source

      """.stripMargin).cache()

     distinct("o_stat_prp_person",o_stat_prp_person,"mobile")

  }


  def distinct(tableName:String,origin:DataFrame,phoneFields:String) = {
    spark.sqlContext.udf.register("rel_uuid",(str:String)=>UUID.randomUUID().toString.replace("-",""))

    origin.createOrReplaceTempView("origin")

    //  IDCard_email_mobile_ISNULL
    val IDCard_email_mobile_ISNULL =  spark.sql(
      s"""
         |  select * from  origin  where  card_code is  null and email is null and  ${phoneFields} is  null
      """.stripMargin).cache()

//    println("     IDCard_email_mobile_ISNULL          "+IDCard_email_mobile_ISNULL.count())


    //IDCard_email_mobile_atLeastOne
    val IDCard_email_mobile_atLeastOne = origin.except(IDCard_email_mobile_ISNULL).cache()
    IDCard_email_mobile_atLeastOne.createOrReplaceTempView("IDCard_email_mobile_atLeastOne")
    spark.sql(s"select rel_uuid(email) as rel_id,*,CleanString(card_code) as clean_card,CleanString(email) as clean_email,CleanString(${phoneFields}) as clean_mobile  from IDCard_email_mobile_atLeastOne")
      .cache().createOrReplaceTempView("temp")

//    println("        IDCard_email_mobile_atLeastOne     "+IDCard_email_mobile_atLeastOne.count())


    // 11111. If the name and id are not null ,Filter by  psn_name  and  card_code
    val idCardNotNull = spark.sql(
      """
        |select *  from temp where psn_name is not null  and  card_code is not null
      """.stripMargin)
    idCardNotNull.createOrReplaceTempView("idCardNotNull")
    val idCardNotNullDistinct = spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by psn_name,clean_card order by start_date desc  ) rank from idcardNotNull
        | )a where rank =1
      """.stripMargin)
    idCardNotNullDistinct.createOrReplaceTempView("idCardNotNullDistinct")


    val old_new_card = spark.sql(
      """
        |select a.rel_id as old,b.rel_id as new from idCardNotNull a left join idCardNotNullDistinct b  on a.psn_name= b.psn_name and a.clean_card=b.clean_card
      """.stripMargin)
    old_new_card.createOrReplaceTempView("old_new_card")


    val idCardNull = spark.sql(
      """
        |select *  from temp where psn_name is  null  or  card_code is  null
      """.stripMargin)
    val idCardFilter = idCardNotNullDistinct.drop("rank").union(idCardNull).cache()
    idCardFilter.createOrReplaceTempView("idCardFilter")

//    println("             idCardFilter         "+idCardFilter.count())


    // If the name and email are not null ,Filter by  psn_name  and  email
    val emailNotNull = spark.sql(
      """
        |select * from idCardFilter where psn_name is not null and email is not null
      """.stripMargin)
    emailNotNull.createOrReplaceTempView("emailNotNull")
    val emailNotNullDistinct =  spark.sql(
      """
        |select * from (
        |select *, row_number() over (partition by psn_name,clean_email order by start_date desc ) rank from emailNotNull
        | )a where rank =1
      """.stripMargin)
    emailNotNullDistinct.createOrReplaceTempView("emailNotNullDistinct")


    val old_new_email = spark.sql(
      """
        |select a.rel_id as old,b.rel_id as new from emailNotNull a left join emailNotNullDistinct b  on a.psn_name= b.psn_name and a.clean_email=b.clean_email
      """.stripMargin)
    old_new_email.createOrReplaceTempView("old_new_email")


    val old_new_card_email = spark.sql(
      """
        |select * from (
        |select old,if(new_b is null ,new ,new_b) as new  from (
        |select a.old,a.new ,b.new  as new_b from old_new_card a left join old_new_email b on a.new  = b.old
        |)a   union select * from  old_new_email )a group by old,new
        |
      """.stripMargin)

    old_new_card_email.createOrReplaceTempView("old_new_card_email")


    val emailNull = spark.sql(
      """
        |select * from idCardFilter where psn_name is  null or  email is null
      """.stripMargin)


    val emailFilter = emailNotNullDistinct.drop("rank").union(emailNull).cache()
    emailFilter.createOrReplaceTempView("emailFilter")

//    println("     emailFilter        " + emailFilter.count())

    println("-------phone ")


    // If the name and mobile  are not null ,Filter by  psn_name  and  mobile
    val phoneNotNull = spark.sql(
      s"""
         |select * from emailFilter where psn_name is not null and ${phoneFields} is not null
      """.stripMargin)
    phoneNotNull.createOrReplaceTempView("phoneNotNull")
    val phoneNotNullDistinct = spark.sql(
      s"""
         |select * from (
         |select *, row_number() over (partition by psn_name,clean_mobile order by start_date desc ) rank from phoneNotNull
         | )a where rank =1
      """.stripMargin)
    phoneNotNullDistinct.createOrReplaceTempView("phoneNotNullDistinct")



    val old_new_mobile = spark.sql(
      s"""
         |select a.rel_id as old,b.rel_id as new from phoneNotNull a left join phoneNotNullDistinct b  on a.psn_name= b.psn_name and a.clean_mobile=b.clean_mobile
      """.stripMargin)
    old_new_mobile.createOrReplaceTempView("old_new_mobile")

    val old_new_card_email_mobile = spark.sql(
      """
        |select * from (
        |select old,if(new_b is null ,new ,new_b) as new  from (
        |select a.old,a.new ,b.new  as new_b from old_new_card_email a left join old_new_mobile b on a.new  = b.old
        |)a   union select * from  old_new_mobile )a group by old,new
        |
      """.stripMargin).cache()
    old_new_card_email_mobile.createOrReplaceTempView("old_new_card_email_mobile")
//    println("old_new_card_email_mobile        "+old_new_card_email_mobile.count())


    val phoneNull = spark.sql(
      s"""
         |select * from emailFilter where psn_name is  null or ${phoneFields} is  null
      """.stripMargin)
    val phoneFilter = phoneNotNullDistinct.drop("rank").union(phoneNull).cache()

    phoneFilter.createOrReplaceTempView("phoneFilter")

//    println("          phoneFilter      "+phoneFilter.count())


    // 2222. After filtering  ,Generate unique field (key)
    var keyString = "uuidKey(prp_code,psn_name,email,card_code,mobile,prof_title,tel,birthday,'prp') as key"

    if(tableName.equals("o_stat_prp_person")){
      println("o_stat_prp_person")
      keyString = "uuidKey(prp_code,psn_name,email,card_code,mobile,prof_title,tel,birthday,'prp') as key"
    }
    if(tableName.equals("o_pj_member")){
      println("o_pj_member")
      keyString = "uuidKey(prj_code,psn_name,gender,email,card_code,tel,prof_title,birthday,'pj') as key"
    }

    val IDCard_email_mobile_Filter_key = spark.sql(
      s"""
         |select ${keyString},* from phoneFilter
      """.stripMargin).cache()


    // card_code and email and  mobile are null ,Generate unique field (key)
    IDCard_email_mobile_ISNULL.createOrReplaceTempView("IDCard_email_mobile_ISNULL")

    val IDCard_email_mobile_ISNULL_key = spark.sql(
      s"""
         |select ${keyString},* from IDCard_email_mobile_ISNULL
      """.stripMargin).cache()


    // ##############################################  old_new_card_email_mobile
    if(tableName.equals("o_stat_prp_person")){
      o_stat_prp_personCompareWithPerson(IDCard_email_mobile_Filter_key,IDCard_email_mobile_ISNULL_key)
    }
    if(tableName.equals("o_pj_member")){
      o_pj_memberCompareWithPerson(IDCard_email_mobile_Filter_key,IDCard_email_mobile_ISNULL_key)
    }

  }

  def o_pj_memberCompareWithPerson(IDCard_email_mobile_Filter_key_pj_member:DataFrame,IDCard_email_mobile_ISNULL_key_pj_member:DataFrame): Unit = {

    println("   o_pj_memberCompareWithPerson   ")

    val o_person = spark.sql(
      s"""
         |select
         |      a.psn_code
         |      ,a.org_code
         |      ,a.org_name
         |      ,a.dept_code
         |      ,a.zh_name
         |      ,a.card_type
         |      ,CleanCard(card_code) as card_code
         |      ,if (a.card_type = 0,CleanCard(card_code),null) as zero
         |      ,if (a.card_type = 1,CleanCard(card_code),null) as identity_card
         |      ,if (a.card_type = 2,CleanCard(card_code),null) as military_id
         |      ,if (a.card_type = 3,CleanCard(card_code),null) as passport
         |      ,if (a.card_type = 4,CleanCard(card_code),null) as four
         |      ,if (a.card_type = 5,CleanCard(card_code),null) as home_return_permit
         |      ,if (a.card_type = 6,CleanCard(card_code),null) as mainland_travel_permit_for_taiwan_residents
         |      ,a.mobile
         |      ,a.email
         |      ,a.status
         |      ,a.create_date
         |      ,a.title
         |      ,a.prof_title
         |      ,a.penable
         |      ,a.edited
         |      ,a.prof_title_id
         |      ,a.update_time
         |      ,a.tel
         |      ,b.gender
         |      ,b.birthday
         |      ,b.ethnicity
         |      ,b.regioncode
         |      ,b.position
         |      ,b.degreecode
         |      ,b.degreeyear
         |      ,b.degreecountry
         |      ,b.major
         |      ,b.fax
         |      ,b.backupemail
         |      ,b.address
         |      ,b.postcode
         |      ,b.province
         |      ,b.city
         |      ,b.valid_email
         |      ,b.valid_mobile
         |      ,b.valid_card
         |      ,b.login_time
         |      ,b.scholarmate_url
         |      ,b.is_academician
         |      ,b.is_outstandingyouth
         |      ,if(1=1,'o_person','o_person')as source
         |      ,CleanString(CleanCard(card_code)) as clean_card
         |      ,CleanString(email) as clean_email
         |      ,CleanString(mobile) as clean_mobile
         | from origin.o_person a
         | left join  origin.o_person_sub b
         | on a.psn_code = b.psn_code
      """.stripMargin).cache()
    o_person.createOrReplaceTempView("o_person")

    // 22222.  Compare with the person table ,If it doesn't exist,add new data
    val t_person = spark.sql("select *, null as start_date from o_person").cache()
    t_person.createOrReplaceTempView("t_person")


    IDCard_email_mobile_Filter_key_pj_member.createOrReplaceTempView("IDCard_email_mobile_Filter_key_pj_member")
    //Compare by psn_name and  card_code
    spark.sql(
      """
        |select * from IDCard_email_mobile_Filter_key_pj_member where psn_name is not null and  card_code is not null
      """.stripMargin).createOrReplaceTempView("personIDCardNotNULL")

    val card_exists = spark.sql(
      """
        |select * from (
        |    select rel_id,psn_code from (
        |            select  *  from personIDCardNotNULL  a where  exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_card = b.clean_card))a
        |     left join t_person b on a.psn_name=b.zh_name and a.clean_card = b.clean_card
        | )a group by rel_id,psn_code
        |
      """.stripMargin)
    //    println("------card_exists "+ card_exists.count())



    val except_card = spark.sql(
      """
        |select  * from personIDCardNotNULL  a where not exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_card = b.clean_card)
      """.stripMargin).union(spark.sql("select * from IDCard_email_mobile_Filter_key_pj_member where psn_name is  null or card_code is  null"))
    except_card.createOrReplaceTempView("except_card")



    //Compare by psn_name and  email
    spark.sql(
      """
        |select * from except_card where psn_name is not null and  email is not null
      """.stripMargin).createOrReplaceTempView("person_EmailNotNULL")



    val email_exists = spark.sql(
      """
        |select * from (
        |select rel_id,psn_code from (
        |select  * from person_EmailNotNULL  a where  exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_email = b.clean_email))a
        | left join t_person b on a.psn_name=b.zh_name and a.clean_email = b.clean_email
        | )a group by rel_id,psn_code
        |
      """.stripMargin)
    //    println("email_exists--------"+email_exists.count())



    val except_email = spark.sql(
      """
        |select  * from person_EmailNotNULL  a where not exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_email = b.clean_email)
      """.stripMargin).union(spark.sql("select * from except_card where psn_name is  null or email is  null"))
    except_email.createOrReplaceTempView("except_email")




    //Compare by psn_name and  mobile
    spark.sql(
      """
        |select * from except_email where psn_name is not null and  tel is not null
      """.stripMargin).createOrReplaceTempView("person_mobileNotNULL")

    val mobile_exists = spark.sql(
      """
        |select * from (
        |select rel_id,psn_code from (
        |select  * from person_mobileNotNULL  a where  exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_mobile = b.clean_mobile))a
        | left join t_person b on a.psn_name=b.zh_name and a.clean_mobile = b.clean_mobile
        | )a group by rel_id,psn_code
        |
      """.stripMargin)
    //    println("mobile_exists--------"+mobile_exists.count())

    val except_mobile = spark.sql(
      """
        |select  * from person_mobileNotNULL  a where not exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_mobile = b.clean_mobile)
      """.stripMargin).union(spark.sql("select * from except_email where psn_name is  null or tel is  null")).cache()
    except_mobile.createOrReplaceTempView("except_mobile")


    val rel_uuid_psn_code = spark.sql("select rel_id ,key as psn_code from except_mobile").union(card_exists).union(email_exists).union(mobile_exists).cache()
    rel_uuid_psn_code.createOrReplaceTempView("rel_uuid_psn_code")


    println("rel_uuid_psn_code")
    val rel_df = spark.sql(
      """
        |select  old,psn_code from (
        |select a.old,b.psn_code from old_new_card_email_mobile a left join rel_uuid_psn_code b on a.new = b.rel_id
        |)a group by old,psn_code
      """.stripMargin).cache()
    rel_df.createOrReplaceTempView("rel_df")



    IDCard_email_mobile_ISNULL_key_pj_member.createOrReplaceTempView("IDCard_email_mobile_ISNULL_key_pj_member")
    val t_project = spark.sql("select a.prj_code,b.psn_code,null as org_code,a.org_name,a.prof_title,'true' as exactRule  from temp a left join rel_df b on a.rel_id = b.old")
      .union(spark.sql("select prj_code,key ,org_code,org_name, prof_title,'true' as exactRule  from IDCard_email_mobile_ISNULL_key_pj_member"))


    t_project.orderBy("psn_code").show()
    println(t_project.count())

    t_project.createOrReplaceTempView("t_project")

    spark.sqlContext.udf.register("uuid",(str:String)=>UUID.randomUUID().toString.replace("-",""))
    println(t_project.count())
    spark.sql("select * from t_project where psn_code is null").show()
    spark.sql("select * from t_project where prj_code is null").show()




    // m_project_person
    println("pj_member write to  m_project_person")
    spark.sql(s"insert into middle.m_project_person select uuid(prj_code) as id,* from t_project")

    // merge to t_person
    val t_person_pj_member = spark.sql(
      """
        |  select
        |      key  as psn_code
        |     ,org_code
        |     ,org_name
        |     ,if(1=1,null,null) as dept_code
        |     ,psn_name as zh_name
        |     ,card_type
        |     ,card_code
        |     ,if (card_type = 0,card_code ,null) as zero
        |     ,if (card_type = 1,card_code ,null) as identity_card
        |     ,if (card_type = 2,card_code ,null) as military_id
        |     ,if (card_type = 3,card_code ,null) as passport
        |     ,if (card_type = 4,card_code ,null) as four
        |     ,if (card_type = 5,card_code ,null) as home_return_permit
        |     ,if (card_type = 6,card_code ,null) as mainland_travel_permit_for_taiwan_residents
        |     ,if(1=1,null,null) as mobile
        |     ,email
        |     ,if(1=1,null,null) as status
        |     ,if(1=1,null,null) as create_date
        |     ,if(1=1,null,null) as title
        |     ,prof_title
        |     ,if(1=1,null,null) as penable
        |     ,if(1=1,null,null) as edited
        |     ,if(1=1,null,null) as prof_title_id
        |     ,if(1=1,null,null) as update_time
        |     ,tel
        |     ,gender
        |     ,birthday
        |     ,if(1=1,null,null) as ethnicity
        |     ,if(1=1,null,null) as regioncode
        |     ,if(1=1,null,null) as position
        |     ,if(1=1,null,null) as degreecode
        |     ,if(1=1,null,null) as degreeyear
        |     ,if(1=1,null,null) as degreecountry
        |     ,if(1=1,null,null) as major
        |     ,if(1=1,null,null) as fax
        |     ,if(1=1,null,null) as backupemail
        |     ,if(1=1,null,null) as address
        |     ,if(1=1,null,null) as postcode
        |     ,if(1=1,null,null) as province
        |     ,if(1=1,null,null) as city
        |     ,if(1=1,null,null) as valid_email
        |     ,if(1=1,null,null) as valid_mobile
        |     ,if(1=1,null,null) as valid_card
        |     ,if(1=1,null,null) as login_time
        |     ,if(1=1,null,null) as scholarmate_url
        |     ,if(1=1,null,null) as is_academician
        |     ,if(1=1,null,null) as is_outstandingyouth
        |     ,source
        |     ,start_date
        |   from except_mobile
      """.stripMargin).union(t_person.drop("clean_card").drop("clean_email").drop("clean_mobile")).cache()
    t_person_pj_member.createOrReplaceTempView("t_person_pj_member")



    //t_person_stat_prp_person_pj_memberADD
    println("write to m_person_pj_member")
    spark.sql(s"insert into middle.m_person_pj_member select * from t_person_pj_member")

    IDCard_email_mobile_ISNULL_key_pj_member.createOrReplaceTempView("IDCard_email_mobile_ISNULL_key_pj_member")
    // IDCard_email_mobile_ISNULL_key
    println("t_person_pj_member_null   write  to m_person")
    spark.sql(
      """
        |  insert into middle.m_person select * from(
        |  select
        |      key  as psn_code
        |     ,org_code
        |     ,org_name
        |     ,if(1=1,null,null) as dept_code
        |     ,psn_name as zh_name
        |     ,card_type
        |     ,card_code
        |     ,if (card_type = 0,card_code ,null) as zero
        |     ,if (card_type = 1,card_code ,null) as identity_card
        |     ,if (card_type = 2,card_code ,null) as military_id
        |     ,if (card_type = 3,card_code ,null) as passport
        |     ,if (card_type = 4,card_code ,null) as four
        |     ,if (card_type = 5,card_code ,null) as home_return_permit
        |     ,if (card_type = 6,card_code ,null) as mainland_travel_permit_for_taiwan_residents
        |     ,if(1=1,null,null) as mobile
        |     ,email
        |     ,if(1=1,null,null) as status
        |     ,if(1=1,null,null) as create_date
        |     ,if(1=1,null,null) as title
        |     ,prof_title
        |     ,if(1=1,null,null) as penable
        |     ,if(1=1,null,null) as edited
        |     ,if(1=1,null,null) as prof_title_id
        |     ,if(1=1,null,null) as update_time
        |     ,tel
        |     ,gender
        |     ,birthday
        |     ,if(1=1,null,null) as ethnicity
        |     ,if(1=1,null,null) as regioncode
        |     ,if(1=1,null,null) as position
        |     ,if(1=1,null,null) as degreecode
        |     ,if(1=1,null,null) as degreeyear
        |     ,if(1=1,null,null) as degreecountry
        |     ,if(1=1,null,null) as major
        |     ,if(1=1,null,null) as fax
        |     ,if(1=1,null,null) as backupemail
        |     ,if(1=1,null,null) as address
        |     ,if(1=1,null,null) as postcode
        |     ,if(1=1,null,null) as province
        |     ,if(1=1,null,null) as city
        |     ,if(1=1,null,null) as valid_email
        |     ,if(1=1,null,null) as valid_mobile
        |     ,if(1=1,null,null) as valid_card
        |     ,if(1=1,null,null) as login_time
        |     ,if(1=1,null,null) as scholarmate_url
        |     ,if(1=1,null,null) as is_academician
        |     ,if(1=1,null,null) as is_outstandingyouth
        |     ,source
        |     ,start_date
        |   from IDCard_email_mobile_ISNULL_key_pj_member)a
      """.stripMargin)



  }

  def o_stat_prp_personCompareWithPerson(IDCard_email_mobile_Filter_key:DataFrame,IDCard_email_mobile_ISNULL_key:DataFrame) = {

    println(" -      -------o_stat_prp_personCompareWithPerson            +++++++++++++++++++++")
//    println(spark.sql("select * from temp").count())


    val t_person = spark.sql("select *  ,CleanString(card_code) as clean_card,CleanString(email) as clean_email ,CleanString(tel) as clean_mobile  from middle.m_person_pj_member")
    t_person.createOrReplaceTempView("t_person")


    //Compare by psn_name and  card_code
    IDCard_email_mobile_Filter_key.createOrReplaceTempView("IDCard_email_mobile_Filter_key")
    spark.sql(
      """
        |select * from IDCard_email_mobile_Filter_key where psn_name is not null and  card_code is not null
      """.stripMargin).createOrReplaceTempView("personIDCardNotNULL")

    val card_exists = spark.sql(
      """
        |select * from (
        |    select rel_id,psn_code from (
        |            select  *  from personIDCardNotNULL  a where  exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_card = b.clean_card))a
        |     left join t_person b on a.psn_name=b.zh_name and a.clean_card = b.clean_card
        | )a group by rel_id,psn_code
        |
      """.stripMargin)
    //    println("------card_exists "+ card_exists.count())


    val except_card = spark.sql(
      """
        |select  * from personIDCardNotNULL  a where not exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_card = b.clean_card)
      """.stripMargin).union(spark.sql("select * from IDCard_email_mobile_Filter_key where psn_name is  null or card_code is  null"))
    except_card.createOrReplaceTempView("except_card")


    //Compare by psn_name and  email
    spark.sql(
      """
        |select * from except_card where psn_name is not null and  email is not null
      """.stripMargin).createOrReplaceTempView("person_EmailNotNULL")


    val email_exists = spark.sql(
      """
        |select * from (
        |select rel_id,psn_code from (
        |select  * from person_EmailNotNULL  a where  exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_email = b.clean_email))a
        | left join t_person b on a.psn_name=b.zh_name and a.clean_email = b.clean_email
        | )a group by rel_id,psn_code
        |
      """.stripMargin)
    //    println("email_exists--------"+email_exists.count())


    val except_email = spark.sql(
      """
        |select  * from person_EmailNotNULL  a where not exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_email = b.clean_email)
      """.stripMargin).union(spark.sql("select * from except_card where psn_name is  null or email is  null"))
    except_email.createOrReplaceTempView("except_email")

    //Compare by psn_name and  mobile
    spark.sql(
      """
        |select * from except_email where psn_name is not null and  mobile is not null
      """.stripMargin).createOrReplaceTempView("person_mobileNotNULL")

    val mobile_exists = spark.sql(
      """
        |select * from (
        |select rel_id,psn_code from (
        |select  * from person_mobileNotNULL  a where  exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_mobile = b.clean_mobile))a
        | left join t_person b on a.psn_name=b.zh_name and a.clean_mobile = b.clean_mobile
        | )a group by rel_id,psn_code
        |
      """.stripMargin)
    //    println("mobile_exists--------"+mobile_exists.count())


    val except_mobile = spark.sql(
      """
        |select  * from person_mobileNotNULL  a where not exists (select * from t_person b where a.psn_name=b.zh_name and a.clean_mobile = b.clean_mobile)
      """.stripMargin).union(spark.sql("select * from except_email where psn_name is  null or mobile is  null")).cache()
    except_mobile.createOrReplaceTempView("except_mobile")


    val rel_uuid_psn_code = spark.sql("select rel_id ,key as psn_code from except_mobile").union(card_exists).union(email_exists).union(mobile_exists).cache()
    rel_uuid_psn_code.createOrReplaceTempView("rel_uuid_psn_code")


    println(spark.sql("select * from old_new_card_email_mobile").count())

    println("rel_uuid_psn_code")
    val rel_df = spark.sql(
      """
        |select  old,psn_code from (
        |select a.old,b.psn_code from old_new_card_email_mobile a left join rel_uuid_psn_code b on a.new = b.rel_id
        |)a group by old,psn_code
      """.stripMargin).cache()
    rel_df.createOrReplaceTempView("rel_df")


    IDCard_email_mobile_ISNULL_key.createOrReplaceTempView("IDCard_email_mobile_ISNULL_key")

    val t_proposal = spark.sql("select a.prp_code,b.psn_code,a.seq_no,null as org_code,a.org_name,a.prof_title,'true' as exactRule  from temp a left join rel_df b on a.rel_id = b.old")
      .union(spark.sql("select prp_code,key ,seq_no,null as org_code,org_name, prof_title,'true' as exactRule  from IDCard_email_mobile_ISNULL_key")).cache()

    t_proposal.createOrReplaceTempView("t_proposal")
    t_proposal.orderBy("psn_code").show()



    //    println(except_mobile.count())

    val t_person_stat_prp_personADD = spark.sql(
      """
        |  select
        |      key  as psn_code
        |     ,if(1=1,null,null) as  org_code
        |     ,org_name
        |     ,if(1=1,null,null) as dept_code
        |     ,psn_name as zh_name
        |     ,card_type
        |     ,card_code
        |     ,if (card_type = 0,card_code ,null) as zero
        |     ,if (card_type = 1,card_code ,null) as identity_card
        |     ,if (card_type = 2,card_code ,null) as military_id
        |     ,if (card_type = 3,card_code ,null) as passport
        |     ,if (card_type = 4,card_code ,null) as four
        |     ,if (card_type = 5,card_code ,null) as home_return_permit
        |     ,if (card_type = 6,card_code ,null) as mainland_travel_permit_for_taiwan_residents
        |     ,mobile
        |     ,email
        |     ,if(1=1,null,null) as status
        |     ,if(1=1,null,null) as create_date
        |     ,if(1=1,null,null) as title
        |     ,prof_title
        |     ,if(1=1,null,null) as penable
        |     ,if(1=1,null,null) as edited
        |     ,if(1=1,null,null) as prof_title_id
        |     ,if(1=1,null,null) as update_time
        |     ,tel
        |     ,gender
        |     ,birthday
        |     ,if(1=1,null,null) as ethnicity
        |     ,if(1=1,null,null) as regioncode
        |     ,if(1=1,null,null) as position
        |     ,if(1=1,null,null) as degreecode
        |     ,if(1=1,null,null) as degreeyear
        |     ,if(1=1,null,null) as degreecountry
        |     ,if(1=1,null,null) as major
        |     ,if(1=1,null,null) as fax
        |     ,if(1=1,null,null) as backupemail
        |     ,if(1=1,null,null) as address
        |     ,if(1=1,null,null) as postcode
        |     ,if(1=1,null,null) as province
        |     ,if(1=1,null,null) as city
        |     ,if(1=1,null,null) as valid_email
        |     ,if(1=1,null,null) as valid_mobile
        |     ,if(1=1,null,null) as valid_card
        |     ,if(1=1,null,null) as login_time
        |     ,if(1=1,null,null) as scholarmate_url
        |     ,if(1=1,null,null) as is_academician
        |     ,if(1=1,null,null) as is_outstandingyouth
        |     ,source
        |     ,start_date
        |   from except_mobile
      """.stripMargin).union(t_person.drop("clean_card").drop("clean_email").drop("clean_mobile")).cache()
    t_person_stat_prp_personADD.createOrReplaceTempView("t_person_stat_prp_personADD")

    println("write to m_person_stat_prp_person")
    spark.sql(s"insert into middle.m_person select * from t_person_stat_prp_personADD")


    // Mapping relation table m_proposal_person
    spark.sqlContext.udf.register("uuid",(str:String)=>UUID.randomUUID().toString.replace("-",""))
    // m_proposal_person
    println("stat_prp_person  write  to  m_proposal_person")
    spark.sql(s"insert into middle.m_proposal_person select uuid(prp_code) as id,* from t_proposal")

    //    IDCard_email_mobile_ISNULL_key    t_person_stat_prp_person_null  122499
    IDCard_email_mobile_ISNULL_key.createOrReplaceTempView("IDCard_email_mobile_ISNULL_key")


    println("t_person_stat_prp_person_null  write  to m_person")
    spark.sql(
      """
        |  insert into middle.m_person select * from(
        |  select
        |      key  as psn_code
        |     ,if(1=1,null,null) as  org_code
        |     , org_name
        |     ,if(1=1,null,null) as dept_code
        |     ,psn_name as zh_name
        |     ,card_type
        |     ,card_code
        |     ,if (card_type = 0,card_code ,null) as zero
        |     ,if (card_type = 1,card_code ,null) as identity_card
        |     ,if (card_type = 2,card_code ,null) as military_id
        |     ,if (card_type = 3,card_code ,null) as passport
        |     ,if (card_type = 4,card_code ,null) as four
        |     ,if (card_type = 5,card_code ,null) as home_return_permit
        |     ,if (card_type = 6,card_code ,null) as mainland_travel_permit_for_taiwan_residents
        |     ,mobile
        |     ,email
        |     ,if(1=1,null,null) as status
        |     ,if(1=1,null,null) as create_date
        |     ,if(1=1,null,null) as title
        |     , prof_title
        |     ,if(1=1,null,null) as penable
        |     ,if(1=1,null,null) as edited
        |     ,if(1=1,null,null) as prof_title_id
        |     ,if(1=1,null,null) as update_time
        |     ,tel
        |     ,gender
        |     ,birthday
        |     ,if(1=1,null,null) as ethnicity
        |     ,if(1=1,null,null) as regioncode
        |     ,if(1=1,null,null) as position
        |     ,if(1=1,null,null) as degreecode
        |     ,if(1=1,null,null) as degreeyear
        |     ,if(1=1,null,null) as degreecountry
        |     ,if(1=1,null,null) as major
        |     ,if(1=1,null,null) as fax
        |     ,if(1=1,null,null) as backupemail
        |     ,if(1=1,null,null) as address
        |     ,if(1=1,null,null) as postcode
        |     ,if(1=1,null,null) as province
        |     ,if(1=1,null,null) as city
        |     ,if(1=1,null,null) as valid_email
        |     ,if(1=1,null,null) as valid_mobile
        |     ,if(1=1,null,null) as valid_card
        |     ,if(1=1,null,null) as login_time
        |     ,if(1=1,null,null) as scholarmate_url
        |     ,if(1=1,null,null) as is_academician
        |     ,if(1=1,null,null) as is_outstandingyouth
        |     ,source
        |     ,start_date
        |   from IDCard_email_mobile_ISNULL_key)a
      """.stripMargin)
  }


  def md5(prp_code:String,psn_name:String,email:String,card_code:String,mobile:String,prof_title:String,tel:String,birthday:String,source:String): String = {
    val str = prp_code+psn_name+email+card_code+mobile+prof_title+tel+birthday+source
    import java.security.MessageDigest
    val md5 = MessageDigest.getInstance("MD5")
    md5.update(str.getBytes)
    val b = md5.digest
    var i = 0
    val buf = new StringBuffer()

    for (offset <- 0 until b.length){
      i= b(offset)
      if (i<0)  i+=256
      if(i<16) buf.append("0")
      buf.append(Integer.toHexString(i))
    }
    source+buf.toString
  }


}