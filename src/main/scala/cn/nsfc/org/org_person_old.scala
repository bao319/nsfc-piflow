package cn.nsfc.org

import org.apache.spark.sql._


object org_person_old extends Serializable {

    def main(args: Array[String]): Unit = {
        distinct()
    }


    def distinct(): Unit = {
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

        spark.sqlContext.udf.register("clean_String",(str:String)=>{
            str.trim.replace(" ","").toLowerCase
        })


        val distinctORG =  spark.sql("select * ,clean_String(name) as clean_org from middle.m_organization ")
        distinctORG.createOrReplaceTempView("temp")
        println(distinctORG.count())


        // 4699824
        val person = spark.sql("select *,clean_String(org_name) as clean_org from middle.m_person where org_name is not null")
        person.createOrReplaceTempView("person")
        spark.sqlContext.udf.register("uuidKey",(org_name:String,source:String )=>{
            md5(org_name,source)
        })

        //        4396279
        val str_exists: String = clean_sql_String(person,"org_code","b.org_code,")
        val org_exists_person = spark.sql(
            s"""
              |
              | ${str_exists}  from (
              |  select a.* from person a  where  exists (select * from temp b where a.clean_org = b.clean_org)
              |  )a
              | left join temp b on a.clean_org = b.clean_org
              |
            """.stripMargin).drop("clean_org")

        //303545
        val org_not_exists = spark.sql(
            """
              |select * from person a  where not exists (select * from temp b where a.clean_org = b.clean_org)
            """.stripMargin)
        org_not_exists.createOrReplaceTempView("org_not_exists")

//         75023
        val uuid_org_code = spark.sql(
            """
              |select  uuidKey(clean_org,'psn') as org_code_psn ,org_name  ,clean_org  from (
              |  select org_name,clean_org , row_number() over (partition by clean_org order by org_name desc ) rank from org_not_exists
              | )a where rank =1
            """.stripMargin)
        uuid_org_code.createOrReplaceTempView("uuid_org_code")
        println(uuid_org_code.count())


        //        303545
        val str_not_exists: String = clean_sql_String(person,"org_code","b.org_code_psn as org_code,")
        val org_not_exists_person = spark.sql(
          s"""
            |
            | ${str_not_exists}  from  org_not_exists a left join uuid_org_code b on a.clean_org = b.clean_org
            |
          """.stripMargin).drop("clean_org")
        println(org_not_exists_person.count())

        // 4699824
        val person_NEW = org_exists_person.union(org_not_exists_person).union(spark.sql("select *  from middle.m_person  where  org_name is null"))
        person_NEW.createOrReplaceTempView("person_NEW")
        println(person_NEW.count())


//        spark.sql("insert into middle.m_person_org select * from person_NEW")
//        spark.sql("insert into middle.m_organization_add select org_code_psn ,org_name  from uuid_org_code")










    }

    def clean_sql_String(df:DataFrame,fieldString:String,appdedString:String): String = {
        val df_Fields: Array[String] = df.schema.fieldNames
        val df_sqlString = new StringBuilder
        for (i <- 0 until df_Fields.length) {
            if (df_Fields(i).equals(fieldString)) df_sqlString.append(appdedString)
            else df_sqlString.append("a."+df_Fields(i) + ",")
        }
        val sqlString = "select " + df_sqlString.toString.dropRight(1)

        sqlString
    }



    def md5(org_name:String,source:String): String = {
        val str = org_name
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