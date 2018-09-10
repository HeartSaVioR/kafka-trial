package net.heartsavior.kafka.trial

import java.time.Duration

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.IsolationLevel

import scala.collection.JavaConversions._
import scala.collection.mutable
import org.json4s._
import org.json4s.jackson.JsonMethods._

// FIXME: type should be changed to Array[Byte], Array[Byte] when applying to Spark

// TODO: should notify that the change of partitions will break exactly-once for specific batch
// TODO: it depends on retention of tx topic, so if previous tx information is removed due to
// retention, the batch can be rewritten

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
      JLong(pid) <- jValue \\ "partitionId"
      JLong(tp) <- jValue \\ "totalPartitions"
      JLong(eid) <- jValue \\ "epochId"
    } yield TxInfo(qid, pid, tp, eid)

    txInfos match {
      case txInfo :: Nil => txInfo
      case Nil => throw new IllegalArgumentException("JSON is not a format of TxInfo!")
    }
  }
}

class IdempotentKafkaWriter(producerParams: Map[String, Object], consumerParams: Map[String, Object],
                            queryId: String, partitionId: Long, totalPartitions: Long,
                            epochId: Long) {
  import IdempotentKafkaWriter._

  val producer: KafkaProducer[String, String] = initProducerWithTransactionSetup()

  // fencing will happen here and afterwards
  producer.initTransactions()

  // if txAlreadyCommitted is true, no need to do anything for this epoch
  val txAlreadyCommitted: Boolean = checkTxAlreadyCommitted()

  if (!txAlreadyCommitted) {
    producer.beginTransaction()
  }

  def write(topic: String, key: String, value: String): Unit = {
    if (!txAlreadyCommitted) {
      producer.send(new ProducerRecord(topic, key, value))
    }
  }

  def commit(): Unit = {
    if (!txAlreadyCommitted) {
      val txInfo = TxInfo(queryId, partitionId, totalPartitions, epochId)

      producer.send(new ProducerRecord(TX_TOPIC, "", txInfo.toJson))

      producer.commitTransaction()
    }

    producer.close()
  }

  def abort() {
    if (!txAlreadyCommitted) {
      producer.abortTransaction()
    }

    producer.close()
  }

  private def initProducerWithTransactionSetup(): KafkaProducer[String, String] = {
    val newProducerParams = new mutable.HashMap[String, Object]()
    newProducerParams ++= producerParams
    newProducerParams.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
      s"kafka-writer-$queryId-$partitionId-$epochId")
    newProducerParams.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    newProducerParams.put(ProducerConfig.CLIENT_ID_CONFIG, "transactional-producer")

    new KafkaProducer[String, String](newProducerParams)
  }

  private def initConsumerWithReadCommitted(): KafkaConsumer[String, String] = {
    val newConsumerParams = new mutable.HashMap[String, Object]()
    newConsumerParams ++= consumerParams
    newConsumerParams.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED)
    newConsumerParams.put(ConsumerConfig.GROUP_ID_CONFIG, "transactional-consumer")

    new KafkaConsumer[String, String](newConsumerParams)
  }

  private def checkTxAlreadyCommitted(): Boolean = {
    // consumer can be cached
    val consumer = initConsumerWithReadCommitted()

    try {
      // FIXME: only assuming partition 0 - partition by key (queryId, partitionId)
      // FIXME: how to optimize this?
      val partition = new TopicPartition(TX_TOPIC, 0)

      consumer.assign(Seq(partition))

      consumer.seekToEnd(Seq(partition))
      val endOffset = consumer.position(partition)

      val txRecords = new mutable.MutableList[ConsumerRecord[String, String]]()
      var foundEndOffset = false

      consumer.seekToBeginning(Seq(partition))
      while (!foundEndOffset) {
        val fetchedRecords = consumer.poll(Duration.ofSeconds(10))
        if (fetchedRecords.nonEmpty) {
          val iter = fetchedRecords.records(partition).listIterator()
          while (!foundEndOffset && iter.hasNext) {
            val record = iter.next()
            if (record.offset() > endOffset) {
              foundEndOffset = true
            } else {
              txRecords += record
            }
          }
        }
      }

      txRecords.exists(p => isTxForEpoch(p, queryId, partitionId, totalPartitions, epochId))
    } finally {
      consumer.close()
    }
  }

  private def isTxForEpoch(record: ConsumerRecord[String, String], queryId: String,
                           partitionId: Long, totalPartitions: Long, epochId: Long): Boolean = {
    TxInfo.fromJson(record.value()).equals(TxInfo(queryId, partitionId, totalPartitions, epochId))
  }

}

object IdempotentKafkaWriter {
  val TX_TOPIC = "spark-kafka-tx"
}
