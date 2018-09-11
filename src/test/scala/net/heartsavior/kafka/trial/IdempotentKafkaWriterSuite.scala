package net.heartsavior.kafka.trial

import java.nio.file.Files
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.errors.ProducerFencedException
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.scalatest.{FunSuite, PrivateMethodTester}

import scala.collection.JavaConverters._
import scala.collection.mutable

class TestKafkaCluster(numBrokers: Int, brokerProperties: Properties)
  extends EmbeddedKafkaCluster(numBrokers, brokerProperties) {
  override def before(): Unit = super.before()
  override def after(): Unit = super.after()
}

class IdempotentKafkaWriterSuite extends FunSuite with PrivateMethodTester {
  import IdempotentKafkaWriter._

  val txTopicConfig = new Properties()
  txTopicConfig.put("retention.ms", (1000L * 60 * 60 * 24 * 30).toString)

  val tempDirForBroker = Files.createTempDirectory("test-idempotent-kafka-writer")
  println(s"Using $tempDirForBroker as a temporary directory for broker")

  val brokerProperties = {
    val p = new Properties()
    p.put("log.dir", tempDirForBroker.toFile.getCanonicalPath)
    p.put("log.flush.interval.messages", "1")
    p.put("replica.socket.timeout.ms", "1500")
    p.put("group.initial.rebalance.delay.ms", "10")

    // Change the following settings as we have only 1 broker
    p.put("offsets.topic.num.partitions", "1")
    p.put("offsets.topic.replication.factor", "1")
    p.put("transaction.state.log.replication.factor", "1")
    p.put("transaction.state.log.min.isr", "1")
    p
  }

  val kafkaCluster = new TestKafkaCluster(1, brokerProperties)

  var bootstrapServers: String = null

  val testTopics: Seq[String] = Seq("test1", "test2", "test3")

  val txAlreadyCommittedFromOthers = PrivateMethod[Boolean]('txAlreadyCommittedFromOthers)

  override def withFixture(test: NoArgTest) = {
    // Shared setup (run at beginning of each test)
    Files.createDirectories(tempDirForBroker)
    kafkaCluster.before()
    bootstrapServers = kafkaCluster.bootstrapServers()
    kafkaCluster.deleteAllTopicsAndWait(1000 * 60 * 5)

    kafkaCluster.createTopic(TX_TOPIC, 1, 1, txTopicConfig)
    testTopics.foreach(topic => kafkaCluster.createTopic(topic))

    try test()
    finally {
      // Shared cleanup (run at end of each test)
      kafkaCluster.deleteAllTopicsAndWait(1000 * 60 * 5)
      kafkaCluster.after()
      Files.deleteIfExists(tempDirForBroker)
    }
  }

  test("Basic test - only one query - add batch - success") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryId = "test-query-id"
    val testPartitionId = 0
    val testTotalPartitions = 1
    val testEpochId = 1

    val writerForPart1Batch1 = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => writerForPart1Batch1.write(topic, "key_" + topic, "value_" + topic)
    }

    writerForPart1Batch1.commit()

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)


    testTopics.foreach { topic =>
      verifyRecords(consumerProperties, topic, Seq(("key_" + topic, "value_" + topic)))
    }

    verifyTxRecords(consumerProperties, TX_TOPIC,
      Seq(TxInfo(testQueryId, testPartitionId, testTotalPartitions, testEpochId)))
  }

  test("Basic test - only one query - multiple tasks trying to add same batch concurrently") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryId = "test-query-id"
    val testPartitionId = 0
    val testTotalPartitions = 1
    val testEpochId = 1

    val writerForPart1Batch1 = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    val writerForPart1Batch1Alter = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    val writerForPart1Batch1Alter2 = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    verifyProducerFencedException { () =>
      testTopics.foreach {
        topic => writerForPart1Batch1.write(topic, "key_" + topic, "value_" + topic)
      }
    }

    verifyProducerFencedException { () =>
      testTopics.foreach {
        topic => writerForPart1Batch1Alter.write(topic, "key_" + topic, "value_" + topic)
      }
    }

    testTopics.foreach {
      topic => writerForPart1Batch1Alter2.write(topic, "key_" + topic, "value_" + topic)
    }

    verifyProducerFencedException { () =>
      writerForPart1Batch1.commit()
    }

    verifyProducerFencedException { () =>
      writerForPart1Batch1Alter.commit()
    }

    writerForPart1Batch1Alter2.commit()

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)

    testTopics.foreach { topic =>
      verifyRecords(consumerProperties, topic, Seq(("key_" + topic, "value_" + topic)))
    }

    verifyTxRecords(consumerProperties, TX_TOPIC,
      Seq(TxInfo(testQueryId, testPartitionId, testTotalPartitions, testEpochId)))
  }

  test("Basic test - only one query - add a batch which is already committed") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryId = "test-query-id"
    val testPartitionId = 0
    val testTotalPartitions = 1
    val testEpochId = 1

    val writerForPart1Batch1 = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => writerForPart1Batch1.write(topic, "key_" + topic, "value_" + topic)
    }

    writerForPart1Batch1.commit()

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)

    testTopics.foreach { topic =>
      verifyRecords(consumerProperties, topic, Seq(("key_" + topic, "value_" + topic)))
    }

    verifyTxRecords(consumerProperties, TX_TOPIC,
      Seq(TxInfo(testQueryId, testPartitionId, testTotalPartitions, testEpochId)))

    val writerForPart1Batch1Trial2 = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    assert(writerForPart1Batch1Trial2 invokePrivate txAlreadyCommittedFromOthers())

    testTopics.foreach {
      topic => writerForPart1Batch1Trial2.write(topic, "key_" + topic, "value_" + topic)
    }

    writerForPart1Batch1Trial2.commit()

    testTopics.foreach { topic =>
      intercept[AssertionError] {
        IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
          1, 1000L)
      }
    }

    intercept[AssertionError] {
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, TX_TOPIC,
        1, 1000L)
    }
  }

  test("Basic test - multiple queries - add multiple batches from multiple partitions concurrently") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryIds = (0 until 10).map(idx => s"test-query-id-$idx")
    val testPartitionIds = (0 until 10)
    val testTotalPartitions = testPartitionIds.size
    val testEpochIds = (1 to 10)
    val recordsPerEpochAndPartition = 10

    val records: mutable.Queue[(String, String)] = new mutable.Queue[(String, String)]()
    val numTotalTestRecords = testQueryIds.size * testPartitionIds.size * testEpochIds.size *
      recordsPerEpochAndPartition
    (0 until numTotalTestRecords).foreach { idx =>
      records.enqueue((s"key_$idx", s"value_$idx"))
    }
    val expectedRecords = records.toList

    def dequeueRecord(records: mutable.Queue[(String, String)]): (String, String) = {
      records.synchronized {
        records.dequeue()
      }
    }

    val random = scala.util.Random

    val runnables = for {
      queryId <- testQueryIds
      partitionId <- testPartitionIds
    } yield new Runnable {
      override def run(): Unit = {
        Thread.sleep(random.nextInt(10))

        testEpochIds.foreach { epochId =>
          val writer = new IdempotentKafkaWriter(producerPropertiesMap, consumerPropertiesMap,
            queryId, partitionId, testTotalPartitions, epochId)

          Thread.sleep(random.nextInt(10))

          (0 until recordsPerEpochAndPartition).foreach { _ =>
            val record = dequeueRecord(records)
            writer.write(testTopics.head, record._1, record._2)
          }

          Thread.sleep(random.nextInt(10))

          writer.commit()
        }
      }
    }

    val threads = runnables.map { runnable =>
      val t = new Thread(runnable)
      t.start()
      t
    }

    threads.foreach(_.join())

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)

    verifyRecordsWithoutSequence(consumerProperties, testTopics.head, expectedRecords)

    val expectedTxInfos = for {
      queryId <- testQueryIds
      partitionId <- testPartitionIds
      epochId <- testEpochIds
    } yield TxInfo(queryId, partitionId, testTotalPartitions, epochId)

    verifyTxRecordsWithoutSequence(consumerProperties, TX_TOPIC, expectedTxInfos)
  }


  test("Basic test - only one query - add a batch but aborted due to failure") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryId = "test-query-id"
    val testPartitionId = 0
    val testTotalPartitions = 1
    val testEpochId = 1

    val writerForPart1Batch1 = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => writerForPart1Batch1.write(topic, "key_" + topic, "value_" + topic)
    }

    writerForPart1Batch1.abort()

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)

    testTopics.foreach { topic =>
      intercept[AssertionError] {
        IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
          1, 1000L)
      }
    }

    intercept[AssertionError] {
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, TX_TOPIC,
        1, 1000L)
    }

    // committing from another partition with another records

    val anotherWriter = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId + 1, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => anotherWriter.write(topic, "key_another_" + topic, "value_another_" + topic)
    }

    anotherWriter.commit()

    // aborted transaction should not be seen from consumer which enables read_committed

    testTopics.foreach { topic => verifyRecords(consumerProperties, topic,
      Seq(("key_another_" + topic, "value_another_" + topic)))
    }

    verifyTxRecords(consumerProperties, TX_TOPIC,
      Seq(TxInfo(testQueryId, testPartitionId + 1, testTotalPartitions, testEpochId)))
  }

  test("Basic test - only one query - add a batch but aborted, and retry committing to same batch") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryId = "test-query-id"
    val testPartitionId = 0
    val testTotalPartitions = 1
    val testEpochId = 1

    val writer = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => writer.write(topic, "key_" + topic, "value_" + topic)
    }

    writer.abort()

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)

    // no data will be presented

    testTopics.foreach { topic =>
      intercept[AssertionError] {
        IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
          1, 1000L)
      }
    }

    intercept[AssertionError] {
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, TX_TOPIC,
        1, 1000L)
    }

    // committing from same partition with same records

    val retryingWriter = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => retryingWriter.write(topic, "key_" + topic, "value_" + topic)
    }

    retryingWriter.commit()

    // aborted transaction should not be seen from consumer which enables read_committed
    // though the data is same, the date is added via retrying

    testTopics.foreach { topic => verifyRecords(consumerProperties, topic,
      Seq(("key_" + topic, "value_" + topic)))
    }

    verifyTxRecords(consumerProperties, TX_TOPIC,
      Seq(TxInfo(testQueryId, testPartitionId, testTotalPartitions, testEpochId)))
  }

  test("Basic test - add a batch but writer stalled, and other task retries committing to same batch") {
    val producerPropertiesMap: Map[String, Object] = getProducerPropertiesMap()
    val consumerPropertiesMap: Map[String, Object] = getConsumerPropertiesMap()

    val testQueryId = "test-query-id"
    val testPartitionId = 0
    val testTotalPartitions = 1
    val testEpochId = 1

    val writer = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => writer.write(topic, "key_" + topic, "value_" + topic)
    }

    // simulating writer getting stalled: I guess it can cover various of failures,
    // specifically other cases test with abort doesn't cover
    // because Kafka will either abort the transaction or keep stalled transaction being idle

    val consumerProperties: Properties = getConsumerPropertiesForVerification(consumerPropertiesMap)

    // no data will be presented

    testTopics.foreach { topic =>
      intercept[AssertionError] {
        IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
          1, 1000L)
      }
    }

    intercept[AssertionError] {
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, TX_TOPIC,
        1, 1000L)
    }

    // committing from same partition with same records

    val retryingWriter = new IdempotentKafkaWriter(producerPropertiesMap,
      consumerPropertiesMap, testQueryId, testPartitionId, testTotalPartitions, testEpochId)

    testTopics.foreach {
      topic => retryingWriter.write(topic, "key_" + topic, "value_" + topic)
    }

    retryingWriter.commit()

    // stalled transaction should not be seen from consumer which enables read_committed
    // though the data is same, the date is added via retrying, not from stalled transaction

    testTopics.foreach { topic => verifyRecords(consumerProperties, topic,
      Seq(("key_" + topic, "value_" + topic)))
    }

    verifyTxRecords(consumerProperties, TX_TOPIC,
      Seq(TxInfo(testQueryId, testPartitionId, testTotalPartitions, testEpochId)))

    // here one more edge case - stalled writer raises again
    // another producer having same transaction id was gone, and kafka doesn't consider this as
    // invalid one as of now, so we need to check tx info again before committing
    writer.write(testTopics.head, "key_stalled", "value_stalled")

    writer.commit()

    // no data will be written from resurrected writer

    testTopics.foreach { topic =>
      intercept[AssertionError] {
        IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
          1, 1000L)
      }
    }

    intercept[AssertionError] {
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, TX_TOPIC,
        1, 1000L)
    }
  }

  private def verifyRecords(consumerProperties: Properties, topic: String,
                            expectedRecords: Seq[(String, String)]): Unit = {
    val records: util.List[ConsumerRecord[String, String]] =
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
        expectedRecords.size, getSaneWaitTimeForRecords(expectedRecords.size))

    assert(records.size() == expectedRecords.size)
    expectedRecords.zip(records.asScala).foreach { case (expectedRecord, record) =>
      assert(expectedRecord._1 === record.key())
      assert(expectedRecord._2 === record.value())
    }
  }

  private def verifyRecordsWithoutSequence(consumerProperties: Properties, topic: String,
                                           expectedRecords: Seq[(String, String)]): Unit = {
    val records: util.List[ConsumerRecord[String, String]] =
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
        expectedRecords.size, getSaneWaitTimeForRecords(expectedRecords.size))

    assert(records.size() == expectedRecords.size)

    val retrievedSet = records.asScala.map(r => (r.key(), r.value())).toSet
    val expectedSet = expectedRecords.toSet

    assert(retrievedSet === expectedSet)
  }

  private def verifyTxRecords(consumerProperties: Properties, topic: String,
                              expectedTxRecords: Seq[TxInfo]): Unit = {
    val records: util.List[ConsumerRecord[String, String]] =
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
        expectedTxRecords.size, getSaneWaitTimeForRecords(expectedTxRecords.size))

    assert(records.size() == expectedTxRecords.size)
    expectedTxRecords.zip(records.asScala).foreach { case (expectedTxInfo, record) =>
      val txJson = record.value()
      val txInfo = TxInfo.fromJson(txJson)
      assert(txInfo === expectedTxInfo)
    }
  }

  private def verifyTxRecordsWithoutSequence(consumerProperties: Properties, topic: String,
                                             expectedTxRecords: Seq[TxInfo]): Unit = {
    val records: util.List[ConsumerRecord[String, String]] =
      IntegrationTestUtils.waitUntilMinRecordsReceived(consumerProperties, topic,
        expectedTxRecords.size, getSaneWaitTimeForRecords(expectedTxRecords.size))

    assert(records.size() == expectedTxRecords.size)

    val retrievedSet = records.asScala.map(r => TxInfo.fromJson(r.value())).toSet
    val expectedSet = expectedTxRecords.toSet

    assert(retrievedSet === expectedSet)
  }

  private def verifyProducerFencedException[U](f: () => U): Unit = {
    try {
      f()
      fail("KafkaException should be thrown which cause is ProducerFencedException!")
    } catch {
      case e: KafkaException if e.getCause.isInstanceOf[ProducerFencedException] =>
      case _ => fail("KafkaException should be thrown which cause is ProducerFencedException!")
    }
  }

  private def getConsumerPropertiesMap(): Map[String, Object] = {
    Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getCanonicalName,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer].getCanonicalName
    )
  }

  private def getProducerPropertiesMap(): Map[String, Object] = {
    Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getCanonicalName,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> classOf[StringSerializer].getCanonicalName
    )
  }

  private def getConsumerPropertiesForVerification(consumerPropertiesMap: Map[String, Object]) = {
    val consumerProperties = new Properties()
    consumerProperties.putAll(consumerPropertiesMap.asJava)
    consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG,
      IsolationLevel.READ_COMMITTED.toString.toLowerCase)
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "integration_test_groupid")
    consumerProperties
  }

  private def getSaneWaitTimeForRecords(numRecords: Int): Long = {
    60000L + 6000L * (numRecords / 1000)
  }

}
