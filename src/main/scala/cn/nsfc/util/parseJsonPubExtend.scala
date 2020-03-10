package cn.nsfc.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
  * @auther ygang@cnic.cn
  * @create 9/11/19
  */
object parseJsonPubExtend extends Serializable {


  def parseAuthor_piflow(str :String):String={

    val psn_name= new StringBuilder
    val org_name= new StringBuilder
    val email= new StringBuilder
    val is_message= new StringBuilder
    val firsr_author= new StringBuilder
    val is_mine= new StringBuilder

    if (str == null){
      psn_name.append("null"+"#")
      org_name.append("null"+"#")
      email.append("null"+"#")
      is_message.append("null"+"#")
      firsr_author.append("null"+"#")
      is_mine.append("null"+"#")
    }
    else if (str.replace(" ","").startsWith("[{")){
      val jsonArray = JSON.parseArray(str)
      if(jsonArray.size>0) {
        for (i <- 0 until jsonArray.size()) {
          if(jsonArray.get(i).toString.contains("psn_name")){
            val jSONObject = jsonArray.getJSONObject(i)

            psn_name.append(jSONObject.get("psn_name") + "#")
            org_name.append(jSONObject.get("org_name") + "#")
            email.append(jSONObject.get("email") + "#")
            is_message.append(jSONObject.get("is_message") + "#")
            firsr_author.append(jSONObject.get("first_author") + "#")
            is_mine.append(jSONObject.get("is_mine") + "#")
            jSONObject.clear()

          } else {
            psn_name.append("null"+"#")
            org_name.append("null"+"#")
            email.append("null"+"#")
            is_message.append("null"+"#")
            firsr_author.append("null"+"#")
            is_mine.append("null"+"#")
          }
        }
      } else {
        psn_name.append("null"+"#")
        org_name.append("null"+"#")
        email.append("null"+"#")
        is_message.append("null"+"#")
        firsr_author.append("null"+"#")
        is_mine.append("null"+"#")
      }
    } else if (str.replace(" ","").startsWith("{")){
      val jSONObject = JSON.parseObject(str)
      psn_name.append(jSONObject.get("psn_name")+"#")
      org_name.append(jSONObject.get("org_name")+"#")
      email.append(jSONObject.get("email")+"#")
      is_message.append(jSONObject.get("is_message")+"#")
      firsr_author.append(jSONObject.get("first_author")+"#")
      is_mine.append(jSONObject.get("is_mine")+"#")
      jSONObject.clear()
    }
    else {
      psn_name.append("null"+"#")
      org_name.append("null"+"#")
      email.append("null"+"#")
      is_message.append("null"+"#")
      firsr_author.append("null"+"#")
      is_mine.append("null"+"#")
    }

    psn_name.toString().stripSuffix("#") + "≌"+
      org_name.toString().stripSuffix("#")+ "≌"+
    email.toString().stripSuffix("#")+ "≌"+
    is_message.toString().stripSuffix("#")+ "≌"+
      firsr_author.toString().stripSuffix("#")+ "≌"+
      is_mine.toString().stripSuffix("#")

  }





  def parseAuthor(str :String):String={

    val psn_name= new StringBuilder
    val org_name= new StringBuilder
    val email= new StringBuilder
    val is_message= new StringBuilder
    val firsr_author= new StringBuilder
    val is_mine= new StringBuilder

    if (str == null){
      psn_name.append("null"+"#")
      org_name.append("null"+"#")
      email.append("null"+"#")
      is_message.append("null"+"#")
      firsr_author.append("null"+"#")
      is_mine.append("null"+"#")
    }
    else if (str.contains("author\":[{")){
      val jsonArray = JSON.parseObject(str).getJSONArray("author")
      if(jsonArray.size>0) {
        for (i <- 0 until jsonArray.size()) {
          if(jsonArray.get(i).toString.contains("psn_name")){
            val jSONObject = jsonArray.getJSONObject(i)

            psn_name.append(jSONObject.get("psn_name") + "#")
            org_name.append(jSONObject.get("org_name") + "#")
            email.append(jSONObject.get("email") + "#")
            is_message.append(jSONObject.get("is_message") + "#")
            firsr_author.append(jSONObject.get("first_author") + "#")
            is_mine.append(jSONObject.get("is_mine") + "#")
            jSONObject.clear()

          } else {
            psn_name.append("null"+"#")
            org_name.append("null"+"#")
            email.append("null"+"#")
            is_message.append("null"+"#")
            firsr_author.append("null"+"#")
            is_mine.append("null"+"#")
          }
        }
      } else {
        psn_name.append("null"+"#")
        org_name.append("null"+"#")
        email.append("null"+"#")
        is_message.append("null"+"#")
        firsr_author.append("null"+"#")
        is_mine.append("null"+"#")
      }
    } else if (str.contains("author\":{")){
      val jSONObject = JSON.parseObject(str).getJSONObject("author")
      psn_name.append(jSONObject.get("psn_name")+"#")
      org_name.append(jSONObject.get("org_name")+"#")
      email.append(jSONObject.get("email")+"#")
      is_message.append(jSONObject.get("is_message")+"#")
      firsr_author.append(jSONObject.get("first_author")+"#")
      is_mine.append(jSONObject.get("is_mine")+"#")
      jSONObject.clear()
    }
    else {
      psn_name.append("null"+"#")
      org_name.append("null"+"#")
      email.append("null"+"#")
      is_message.append("null"+"#")
      firsr_author.append("null"+"#")
      is_mine.append("null"+"#")
    }

    psn_name.toString().stripSuffix("#") + "≌"+
      org_name.toString().stripSuffix("#")+ "≌"+
      email.toString().stripSuffix("#")+ "≌"+
      is_message.toString().stripSuffix("#")+ "≌"+
      firsr_author.toString().stripSuffix("#")+ "≌"+
      is_mine.toString().stripSuffix("#")

  }



  def pub_extend(str1:String,pub_type_id:String): String ={


    var pub_extendString:String = null
    if(str1 == null){
      return pub_extendString
    }

    if (str1.startsWith("{")){
      val str =str1.replaceAll("≌","")
      val jsonObject = JSON.parseObject(str)
      pub_extendString = assemblyPub_extend_issue(jsonObject)

    }

    if (str1.startsWith("[")){
      val str =str1.replaceAll("≌","")
      val array: JSONArray = JSON.parseArray(str)
      for (i<- 0 until array.size() ){
        val jsonObject: JSONObject = array.getJSONObject(i)

        if(jsonObject.containsKey("pub_type_id") && jsonObject.get("pub_type_id") != null) if (jsonObject.get("pub_type_id").toString == pub_type_id)
          pub_extendString = assemblyPub_extend_issue(jsonObject)

      }
    }

    return pub_extendString

  }


  def assemblyPub_extend_issue(jsonObject:JSONObject): String ={

    var issue_no_01 :String  = null
    var issue_no_02 :String  = null

    if (jsonObject.get("issue_no") != null){
      if (jsonObject.get("issue_no").toString.startsWith("[")) {
        val issueArray  = jsonObject.getJSONArray("issue_no")
        if (issueArray.size > 0) {
          for (i <- 0 until issueArray.size()) {
            val issueObject = issueArray.getJSONObject(i)
            if (issueObject != null && issueObject.containsKey("code")) {
              if (issueObject.get("code") == "01") if (issueObject.containsKey("content") && issueObject.get("content") != null) issue_no_01 = issueObject.get("content").toString
              if (issueObject.get("code") == "02") if (issueObject.containsKey("content") && issueObject.get("content") != null) issue_no_02 = issueObject.get("content").toString
            }
          }
        }
      }
      if (jsonObject.get("issue_no").toString.startsWith("{") && jsonObject.containsKey("code")) {
        if (jsonObject.get("code") == "01") if (jsonObject.get("content") != null) issue_no_01 = jsonObject.get("content").toString
        if (jsonObject.get("code") == "02") if (jsonObject.get("content") != null) issue_no_02 = jsonObject.get("content").toString
      }
    }



    jsonObject.toString + "≌"+ issue_no_01+ "≌"+ issue_no_02

  }

  var num =0
  def parsePub_extend(id:String,str:String,fields:String): String ={


    var pub_extendString:String = null
    var  jsonObject:JSONObject = null
      if(str == null){
      return pub_extendString
    }  else {

      try {
         jsonObject = JSON.parseObject(str)
         pub_extendString = jsonObject.get(fields)+""

      }catch {
        case e : Exception => {
          num = num+1
          println("\n\n id"+id+"\n\n\n\n\n\n\n\n\n\n"+num+"\n\n")
          println(str)
        }
      }

    }

    return pub_extendString



  }





}
