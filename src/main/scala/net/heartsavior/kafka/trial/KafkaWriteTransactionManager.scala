package net.heartsavior.kafka.trial

import java.time.Duration

import net.heartsavior.kafka.trial.IdempotentKafkaWriter.TX_TOPIC
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import org.apache.kafka.common.requests.IsolationLevel
import org.json4s.JsonAST.{JInt, JString}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.util.control.NonFatal

case class TxInfoKafkaKey(queryId: String, partitionId: Long, totalPartitions: Long) {
  import org.json4s.JsonDSL._

  def toJson: String = {
    val json = ("queryId" -> queryId) ~
      ("partitionId" -> partitionId) ~
      ("totalPartitions" -> totalPartitions)
    compact(render(json))
  }
}

case class TxInfo(queryId: String, partitionId: Long, totalPartitions: Long, epochId: Long) {
  import org.json4s.JsonDSL._

  def toJson: String = {
    val json = ("queryId" -> queryId) ~
      ("partitionId" -> partitionId) ~
      ("totalPartitions" -> totalPartitions) ~
      ("epochId" -> epochId)
    compact(render(json))
  }
}

object TxInfo {
  def fromJson(json: String): TxInfo = {
    val jValue = parse(json)

    val txInfos: Seq[TxInfo] = for {
      JString(qid) <- jValue \\ "queryId"
      JInt(pid) <- jValue \\ "partitionId"
      JInt(tp) <- jValue \\ "totalPartitions"
      JInt(eid) <- jValue \\ "epochId"
    } yield TxInfo(qid, pid.intValue(), tp.intValue(), eid.longValue())

    txInfos match {
      case txInfo :: Nil => txInfo
      case Nil => throw new IllegalArgumentException("JSON is not a format of TxInfo!")
    }
  }
}

object KafkaWriteTransactionManager {
  val PARTITION = new TopicPartition(TX_TOPIC, 0)
}

///*
class KafkaWriteTransactionManager(consumerParams: Map[String, Object]) {
  import KafkaWriteTransactionManager._

  @volatile var consumer: KafkaConsumer[String, String] = initConsumerWithReadCommitted()

  val lastCommittedTxMap: mutable.HashMap[TxInfoKafkaKey, Long] =
    new mutable.HashMap[TxInfoKafkaKey, Long]()

  var lastReadOffset: Long = -1

  def checkTxAlreadyCommitted(queryId: String, partitionId: Int, totalPartitions: Int,
                              epochId: Long): Boolean = synchronized {
    withRetryingKafkaException[Boolean](3, 500) { () =>
      consumer.seekToEnd(Seq(PARTITION))
      val endOffset = consumer.position(PARTITION)

      if (endOffset <= 0) {
        // no tx data so far
        return false
      }

      if (lastReadOffset < endOffset) {
        // read through topic
        var lastOffset: Long = -1
        var allConsumed = false

        if (lastReadOffset < 0) {
          consumer.seekToBeginning(Seq(PARTITION))
        } else {
          consumer.seek(PARTITION, lastReadOffset + 1)
        }

        while (!allConsumed) {
          val fetchedRecords = consumer.poll(Duration.ofSeconds(5))
          if (fetchedRecords.nonEmpty) {
            val records = fetchedRecords.records(PARTITION)
            lastOffset = records.last.offset()

            records.foreach { record =>
              try {
                val txInfo = TxInfo.fromJson(record.value())
                val txInfoKey = TxInfoKafkaKey(txInfo.queryId, txInfo.partitionId,
                  txInfo.totalPartitions)

                val lastEpoch: Long = lastCommittedTxMap.getOrElse(txInfoKey, -1)
                if (lastEpoch < txInfo.epochId) {
                  lastCommittedTxMap.put(txInfoKey, txInfo.epochId)
                }
              } catch {
                case NonFatal(_) =>
                  println(s"ERROR: Broken TX information detected at offset ${record.offset()}")
              }
            }

            if (lastOffset >= endOffset) {
              allConsumed = true
            }

            lastReadOffset = lastOffset
          } else {
            // no data... then we are sure we read all the available data for now
            allConsumed = true
          }
        }
      }

      val lastEpochId = lastCommittedTxMap.getOrElseUpdate(
        TxInfoKafkaKey(queryId, partitionId, totalPartitions), -1)

      lastEpochId >= epochId
    }
  }

  def writeTransaction(producer: KafkaProducer[String, String], queryId: String, partitionId: Int,
                       totalPartitions: Int, epochId: Long): Unit = {
    val txInfo = TxInfo(queryId, partitionId, totalPartitions, epochId)
    val txKey = TxInfoKafkaKey(queryId, partitionId, totalPartitions)
    producer.send(new ProducerRecord(TX_TOPIC, txKey.toJson, txInfo.toJson))
  }

  def close(): Unit = {
    consumer.close()
  }

  private def initConsumerWithReadCommitted(): KafkaConsumer[String, String] = {
    val newConsumerParams = new mutable.HashMap[String, Object]()
    newConsumerParams ++= consumerParams
    newConsumerParams.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
      IsolationLevel.READ_COMMITTED.toString.toLowerCase)
    newConsumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional_consumer")
    newConsumerParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val con = new KafkaConsumer[String, String](newConsumerParams)

    // FIXME: only assuming partition 0 - partition by key (queryId, partitionId)
    // FIXME: how to optimize this?
    con.assign(Seq(PARTITION))

    con
  }

  private def withRetryingKafkaException[T](maxRetryCount: Int, sleepMsBetweenRetry: Long)(f: () => T): T = {
    var retryCount = 0
    var lastException: Throwable = null

    do {
      try {
        return f()
      } catch {
        case e: KafkaException =>
          lastException = e
          consumer.close()
          Thread.sleep(sleepMsBetweenRetry)
          consumer = initConsumerWithReadCommitted()
          retryCount += 1
      }
    } while (retryCount <= maxRetryCount)

    throw lastException
  }

}
//*/

/*

class KafkaWriteTransactionManager(consumerParams: Map[String, Object]) {
  import KafkaWriteTransactionManager._

  val lastCommittedTxMap: mutable.HashMap[TxInfoKafkaKey, Long] =
    new mutable.HashMap[TxInfoKafkaKey, Long]()

  var lastReadOffset: Long = -1

  def checkTxAlreadyCommitted(queryId: String, partitionId: Int, totalPartitions: Int,
                              epochId: Long): Boolean = synchronized {
    val consumer: KafkaConsumer[String, String] = initConsumerWithReadCommitted()

    try {
      withRetryingKafkaException[Boolean](3, 500) { () =>
        consumer.seekToEnd(Seq(PARTITION))
        val endOffset = consumer.position(PARTITION)

        if (endOffset <= 0) {
          // no tx data so far
          return false
        }

        if (lastReadOffset < endOffset) {
          // read through topic
          var lastOffset: Long = -1
          var allConsumed = false

          if (lastReadOffset < 0) {
            consumer.seekToBeginning(Seq(PARTITION))
          } else {
            consumer.seek(PARTITION, lastReadOffset)
          }

          while (!allConsumed) {
            val fetchedRecords = consumer.poll(Duration.ofSeconds(5))
            if (fetchedRecords.nonEmpty) {
              val records = fetchedRecords.records(PARTITION)
              lastOffset = records.last.offset()

              records.foreach { record =>
                try {
                  val txInfo = TxInfo.fromJson(record.value())
                  val txInfoKey = TxInfoKafkaKey(txInfo.queryId, txInfo.partitionId,
                    txInfo.totalPartitions)

                  val lastEpoch: Long = lastCommittedTxMap.getOrElse(txInfoKey, -1)
                  if (lastEpoch < txInfo.epochId) {
                    lastCommittedTxMap.put(txInfoKey, txInfo.epochId)
                  }
                } catch {
                  case NonFatal(_) =>
                    System.err.println(s"Broken TX information detected at offset ${record.offset()}")
                }
              }

              if (lastOffset >= endOffset) {
                allConsumed = true
              }

              lastReadOffset = lastOffset
            } else {
              // no data... then we are sure we read all the available data for now
              allConsumed = true
            }
          }
        }

        val lastEpochId = lastCommittedTxMap.getOrElseUpdate(
          TxInfoKafkaKey(queryId, partitionId, totalPartitions), -1)

        lastEpochId >= epochId
      }
    } finally {
      consumer.close()
    }

  }

  def writeTransaction(producer: KafkaProducer[String, String], queryId: String, partitionId: Int,
                       totalPartitions: Int, epochId: Long): Unit = {
    val txInfo = TxInfo(queryId, partitionId, totalPartitions, epochId)
    val txKey = TxInfoKafkaKey(queryId, partitionId, totalPartitions)
    producer.send(new ProducerRecord(TX_TOPIC, txKey.toJson, txInfo.toJson))
  }

  private def initConsumerWithReadCommitted(): KafkaConsumer[String, String] = {
    val newConsumerParams = new mutable.HashMap[String, Object]()
    newConsumerParams ++= consumerParams
    newConsumerParams.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
      IsolationLevel.READ_COMMITTED.toString.toLowerCase)
    newConsumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional_consumer")
    newConsumerParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

    val con = new KafkaConsumer[String, String](newConsumerParams)

    // FIXME: only assuming partition 0 - partition by key (queryId, partitionId)
    // FIXME: how to optimize this?
    con.assign(Seq(PARTITION))

    con
  }

  private def withRetryingKafkaException[T](maxRetryCount: Int, sleepMsBetweenRetry: Long)(f: () => T): T = {
    var retryCount = 0
    var lastException: Throwable = null

    do {
      try {
        return f()
      } catch {
        case e: KafkaException =>
          lastException = e
          //consumer.close()
          Thread.sleep(sleepMsBetweenRetry)
          //consumer = initConsumerWithReadCommitted()
          retryCount += 1
      }
    } while (retryCount <= maxRetryCount)

    throw lastException
  }

}
*/