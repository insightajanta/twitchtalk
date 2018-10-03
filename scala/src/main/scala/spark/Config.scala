package spark

object Config {
  val sparkMaster = "local[*]"
  val kafkaBrokers = "localhost:9092"
  val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
  val chatMessagesS3Location = "s3n://com.ajanta/data/chatfinal/"
  val lcS3Location = "s3n://com.ajanta/data/livechannel/"
  val kafkaTopic = "chatmessage_new"
  val cassandraHost = "localhost"
  val cassandraKeySpace = "twitchspace"
}
