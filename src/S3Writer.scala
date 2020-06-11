import java.io.{File, FileWriter}
import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Timer, TimerTask}

import com.bingocloud.auth.BasicAWSCredentials
import com.bingocloud.services.s3.AmazonS3Client
import com.bingocloud.{ClientConfiguration, Protocol}
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.configuration.Configuration

class S3Writer(accessKey: String, secretKey: String, endpoint: String, bucket: String, keyPrefix: String, period: Int, target: String) extends OutputFormat[String] {
  var timer: Timer = _
  var amazonS3: AmazonS3Client = _
  val simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmm") //日期格式化工具
  var file_map: util.HashMap[String, File] = new util.HashMap[String,File]() //根据关键词保存文件
  var fileWriter_map: util.HashMap[String, FileWriter] = new util.HashMap[String, FileWriter]() //根据关键词保存文件写入工具
  var len_map: util.HashMap[String, Integer] = new util.HashMap[String, Integer]() //根据关键词保存文件长度
  def upload: Unit = {
    this.synchronized {
      var key_set = file_map.keySet().toArray()
      key_set.foreach(key=>{
        var length = len_map.get(key)
        var file = file_map.get(key)
        var fileWriter = fileWriter_map.get(key)
        if (length > 0) {
          fileWriter.close()
          val targetKey = keyPrefix  + key + "/" + file.getName
          amazonS3.putObject(bucket, targetKey, file)
          println("开始上传文件：%s 至 %s 桶的 %s 目录下".format(file.getAbsoluteFile, bucket, targetKey))
        }
      })
    }
  }

  override def configure(configuration: Configuration): Unit = {
    timer = new Timer("S3Writer")
    timer.schedule(new TimerTask() {
      def run(): Unit = {
        upload
      }
    }, 1000, period)
    val credentials = new BasicAWSCredentials(accessKey, secretKey)
    val clientConfig = new ClientConfiguration()
    clientConfig.setProtocol(Protocol.HTTP)
    amazonS3 = new AmazonS3Client(credentials, clientConfig)
    amazonS3.setEndpoint(endpoint)

  }

  override def open(taskNumber: Int, numTasks: Int): Unit = {

  }

  override def writeRecord(it: String): Unit = {
    this.synchronized {
      if (StringUtils.isNoneBlank(it)) {
        var name = Picker.pick(it, target)
        if (!file_map.containsKey(name)) {
          var file = new File(name+ "_" +simpleDateFormat.format(new Date(System.currentTimeMillis())) + ".txt")
          var fileWriter = new FileWriter(file, true)
          file_map.put(name, file)
          fileWriter_map.put(name, fileWriter)
        }
        var fileWriter = fileWriter_map.get(name)
        var length = len_map.getOrDefault(name,0)
        fileWriter.append(it+"\n")
        length += it.length
        len_map.put(name, length)
        fileWriter.flush()
      }
    }
  }


  override def close(): Unit = {
    var keySet = len_map.keySet().toArray()
    keySet.foreach(key=>{
      var fileWriter = fileWriter_map.get(key)
      fileWriter.flush()
      fileWriter.close()
    })
    timer.cancel()
  }
}