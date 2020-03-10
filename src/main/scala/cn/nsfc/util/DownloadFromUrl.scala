package cn.nsfc.util

import java.io.{IOException, InputStream}
import java.net.{HttpURLConnection, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils

object DownloadFromUrl {

  def download(hdfsUrl:String,savePath:String,httpURL:String): Unit = {
    val url=new URL(httpURL)
    val uc:HttpURLConnection=url.openConnection().asInstanceOf[HttpURLConnection]
    uc.setDoInput(true)
    uc.setConnectTimeout(60*1000)
    uc.setReadTimeout(60*1000)
    uc.connect()

    var inputStream: InputStream = null
    var outputStream: FSDataOutputStream = null

    try {
      inputStream = uc.getInputStream()

      val configuration: Configuration = new Configuration()
      configuration.set("dfs.support.append","true")
      configuration.set("fs.defaultFS",hdfsUrl)
      val fs = FileSystem.get(configuration)
      outputStream = fs.create(new Path(hdfsUrl+savePath))


      IOUtils.copyBytes(inputStream, outputStream, 4096, false)

    } catch {
      case e :IOException =>e.printStackTrace()
    }
    finally {
      if(inputStream != null) IOUtils.closeStream(inputStream)
      if(outputStream != null) IOUtils.closeStream(outputStream)
    }

    IOUtils.closeStream(inputStream)
    IOUtils.closeStream(outputStream)


  }

}
