package cn.nsfc.test


import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

object test {

  val spark = SparkSession.builder()
    .master("local[12]")
    .appName("packone_139")
    .config("spark.deploy.mode","client")
    .config("spark.driver.memory", "15g")
    .config("spark.executor.memory", "32g")
    .config("spark.cores.max", "16")
    .config("hive.metastore.uris","thrift://192.168.3.140:9083")
    .enableHiveSupport()
    .getOrCreate()



  def main(args: Array[String]): Unit = {
    spark.sql("select * from  middle.m_person_add_rel").show

    //    fund_new()

  }



  def fund_new()= {

    import java.util


    val hash: util.HashMap[String, String] = new util.HashMap[String,String]()
    hash.put("title","2222")
    hash.put("name","111")

    val hash1: util.HashMap[String, String] = new util.HashMap[String,String]()
    hash1.put("title","author")
    hash1.put("name","zzz")


    val arraylist = new util.ArrayList[util.HashMap[String,String]]()
    arraylist.add(hash)
    arraylist.add(hash1)

    import spark.implicits._

    var df:DataFrame=null

    val it = arraylist.iterator()
    while (it.hasNext){

      val stringToString: util.HashMap[String, String] = it.next()
      val rdd: RDD[util.HashMap[String, String]] = spark.sparkContext.parallelize(Seq(stringToString))

      df = (rdd.map(x=>{
        val bb = new ArrayBuffer[String]
        bb.+=(stringToString.get("title"))
        bb.+=(stringToString.get("name"))
        bb
      }).map(x=>(x(0),x(1))).toDF())


      println(stringToString.get("title"))

    }

    df.show()


    println(1 to 10)




















  }
}
