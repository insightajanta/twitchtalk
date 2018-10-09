package spark

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ForeachWriter, _}

class CassandraSink(sc: SparkContext, tableWriter: CassandraTableWriter) extends ForeachWriter[org.apache.spark.sql.Row] {
  val cassandra = CassandraConnector(sc)
  def open(partitionId: Long, version: Long): Boolean = {
    // open connection - no need to do anything for cassandra as withSessionDo does everything
    true
  }

  def process(row: org.apache.spark.sql.Row) = {
    cassandra.withSessionDo(session => session.execute(tableWriter.getQuery(row)))
  }

  def close(errorOrNull: Throwable): Unit = {
    // close connection - no need to do anything for cassandra as withSessionDo does everything
  }
}
