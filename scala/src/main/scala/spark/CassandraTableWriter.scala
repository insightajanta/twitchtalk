package spark

import org.apache.spark.sql.Row

trait CassandraTableWriter extends Serializable {
  def getQuery(row: Row): String
}

class ChatChannelByMinuteWriter extends CassandraTableWriter {
  override
  def getQuery(row: Row): String = {
    s"""insert into ${Config.cassandraKeySpace}.chat_channel_minute (dt, hours, minutes, channel, count)
           values('${row(0)}', ${row(1)}, ${row(2)}, '${row(3)}', ${row(4)})"""
  }
}

class ChatUsernameByMinuteWriter extends CassandraTableWriter {
  override
  def getQuery(row: Row): String = {
    s"""insert into ${Config.cassandraKeySpace}.chat_username_minute (dt, hours, minutes, username, count)
           values('${row(0)}', ${row(1)}, ${row(2)}, '${row(3)}', ${row(4)})"""
  }
}