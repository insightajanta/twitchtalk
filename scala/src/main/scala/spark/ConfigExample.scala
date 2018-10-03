package spark

object Config {
  val kafkaBrokers = "ec2-52-206-31-163.compute-1.amazonaws.com:9092,ec2-52-87-92-193.compute-1.amazonaws.com:9092,ec2-18-215-104-101.compute-1.amazonaws.com:9092"
  val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
  val chatMessagesS3Location = "s3n://com.ajanta/data/chatmessage_new/"
  val lcS3Location = "s3n://com.ajanta/data/livechannel/"
  val kafkaTopic = "chatmessage_new"
  val cassandraHost = "ec2-18-235-141-237.compute-1.amazonaws.com,ec2-35-174-180-132.compute-1.amazonaws.com,ec2-34-196-140-188.compute-1.amazonaws.com"
  val cassandraKeySpace = "twitchspace"
}
