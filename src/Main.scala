
import java.util.{Properties, UUID}

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

object Main {

  val accessKey = "2D06A1DB15FCE613DC3B"
  val secretKey = "WzJCQjM0NzU3REZENTU5QjAwMjQ2RjBGMzAzRTRBNzY0RUYxNkZCNURd"
  //s3地址
  val endpoint = "scuts3.depts.bingosoft.net:29999"
  //上传到的桶
  val bucket = "sujiaxin"
  //上传文件的路径前缀
  val keyPrefix = "output_filter/"
  //上传数据间隔 单位毫秒
  val period = 5000
  /**
   * 输入的主题名称
   */
  val inputTopic = "sujiaxin_2"
  /*
  * 关键词
  * */
  var target = "destination"
  /**
   * kafka地址
   */
  val bootstrapServers = "bigdata35.depts.bingosoft.net:29035,bigdata36.depts.bingosoft.net:29036,bigdata37.depts.bingosoft.net:29037"
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaProperties = new Properties()
    kafkaProperties.put("bootstrap.servers", bootstrapServers)
    kafkaProperties.put("group.id", UUID.randomUUID().toString)
    kafkaProperties.put("auto.offset.reset", "earliest")
    kafkaProperties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProperties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    val kafkaConsumer = new FlinkKafkaConsumer010[ObjectNode](inputTopic,
//      new JSONKeyValueDeserializationSchema(true), kafkaProperties)
    val kafkaConsumer = new FlinkKafkaConsumer010[String](inputTopic,
      new SimpleStringSchema, kafkaProperties)
    kafkaConsumer.setCommitOffsetsOnCheckpoints(true)
    val inputKafkaStream = env.addSource(kafkaConsumer)
    inputKafkaStream.writeUsingOutputFormat(new S3Writer(accessKey, secretKey, endpoint, bucket, keyPrefix, period, target))
    env.execute()
  }

}
