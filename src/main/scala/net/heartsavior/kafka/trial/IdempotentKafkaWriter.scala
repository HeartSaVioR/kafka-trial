package net.heartsavior.kafka.trial

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.JavaConversions._
import scala.collection.mutable

// FIXME: type should be changed to Array[Byte], Array[Byte] when applying to Spark

// TODO: should notify that the change of partitions will break exactly-once for specific batch
// TODO: it depends on retention of tx topic, so if previous tx information is removed due to
// retention, the batch can be rewritten

class IdempotentKafkaWriter(producerParams: Map[String, Object], consumerParams: Map[String, Object],
                            queryId: String, partitionId: Int, totalPartitions: Int,
                            epochId: Long) {
  private val producer: KafkaProducer[String, String] = initProducerWithTransactionSetup()

  // fencing will start here and until closing producer
  producer.initTransactions()

  // if this is true, no need to do anything for this epoch
  private val txManager = new KafkaWriteTransactionManager(consumerParams)

  private var txAlreadyCommittedFromOthers: Boolean = checkTxAlreadyCommitted()

  if (!txAlreadyCommittedFromOthers) {
    producer.beginTransaction()
  } else {
    producer.close()
  }

  private var txAlreadyCompleted: Boolean = false

  def write(topic: String, key: String, value: String): Unit = {
    checkTransactionAlreadyCompleted()

    if (txAlreadyCommittedFromOthers) {
      return
    }

    producer.send(new ProducerRecord(topic, key, value))
  }

  def commit(): Unit = {
    checkTransactionAlreadyCompleted()

    if (txAlreadyCommittedFromOthers) {
      return
    }

    // check tx again: the transaction may be resurrected from long stall whereas others already
    // wrote same batch and closed connection... in this case, producer fencing doesn't help
    txAlreadyCommittedFromOthers = checkTxAlreadyCommitted()

    if (!txAlreadyCommittedFromOthers) {
//      println(s"DEBUG: Double-checked the batch is not committed yet for query: $queryId / " +
//        s"partition: $partitionId / totalPartitions: $totalPartitions / epoch: $epochId ... committing")
      txManager.writeTransaction(producer, queryId, partitionId, totalPartitions, epochId)
      producer.commitTransaction()
      producer.close()
      txAlreadyCompleted = true
    } else {
//      println(s"DEBUG: Double-checked the batch is committed now via another writer " +
//        s"for query: $queryId / partition: $partitionId / totalPartitions: $totalPartitions " +
//        s"/ epoch: $epochId ... skipping")
    }
  }

  def abort(): Unit = {
    checkTransactionAlreadyCompleted()

    if (txAlreadyCommittedFromOthers) {
      return
    }

    producer.abortTransaction()
    producer.close()
    txAlreadyCompleted = true
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

  private def checkTransactionAlreadyCompleted(): Unit = {
    if (txAlreadyCompleted) {
      throw new IllegalStateException("The transaction for this epoch is already completed!")
    }
  }

  private def checkTxAlreadyCommitted(): Boolean = {
    txManager.checkTxAlreadyCommitted(queryId, partitionId, totalPartitions, epochId)
  }
}

object IdempotentKafkaWriter {
  val TX_TOPIC = "spark-kafka-tx"
}
