package spark

object ConfigExample {
  val kafkaBrokers = "localhost:9092" // this can be a comma separated list in prod env
  val accessKeyId = System.getenv("AWS_ACCESS_KEY_ID")
  val secretAccessKey = System.getenv("AWS_SECRET_ACCESS_KEY")
  val chatMessagesS3Location = "s3n://<s3bucket>/<path>/"
  val lcS3Location = "s3n://<s3bucket>/<path>/"
  val kafkaTopic = "<kafka topic>"
  val cassandraHost = "localhost" // this can be a comma separated list in prod env
  val cassandraKeySpace = "twitchspace"
  val sparkMaster = "localhost"
}