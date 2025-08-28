package org.example

import com.rabbitmq.client.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.random.Random
import kotlin.random.nextInt

class CoroutineRabbitMQConsumer(
    private val scope: CoroutineScope,
    private val channel: Channel,
    private val consumerId: Int
) {
    private val logger = LoggerFactory.getLogger("Consumer-$consumerId")
    private val isRunning = AtomicBoolean(true)

    suspend fun startConsuming(queueName: String) {
        val consumer = object : DefaultConsumer(channel) {
            override fun handleDelivery(
                consumerTag: String?,
                envelope: Envelope?,
                properties: AMQP.BasicProperties?,
                body: ByteArray?
            ) {
                scope.launch {
                    try {
                        processMessage(body, envelope)
                    } catch (e: Exception) {
                        logger.warn("Could not process message, rejecting: ${e.message}")
                        channel.basicReject(envelope?.deliveryTag ?: 0, false)
                    }
                }
            }
        }

        channel.basicQos(1) // Fair dispatch
        channel.basicConsume(queueName, false, consumer)
        logger.info("Consumer $consumerId started")

        // Keep the coroutine alive until stopped
        while (isRunning.get()) {
            delay(1000)
        }
    }

    private suspend fun processMessage(body: ByteArray?, envelope: Envelope?) {
        withContext(Dispatchers.IO) {
            val message = String(body ?: byteArrayOf())
            logger.info("START processing message: $message")

            // Simulate IO work with delay
            delay((100 + 10*consumerId).toLong())

            val randInt = Random.nextInt(1..10)
            if (randInt == 10) { // Simulate graceful failure
                logger.info("REQUEUE message $message")
                channel.basicNack(envelope?.deliveryTag ?: 0, false, true) // requeue
                return@withContext
            } else if (randInt == 9) { // Simulate exception
                logger.info("ERROR on message $message")
                throw Exception("error from $consumerId on message $message")
            }

            channel.basicAck(envelope?.deliveryTag ?: 0, false)
            logger.info("FINISHED processing message: $message")
        }
    }

    fun stop() {
        isRunning.set(false)
    }
}

suspend fun main() = coroutineScope {
    val factory = ConnectionFactory().apply {
        host = "localhost"
        username = "guest"
        password = "guest"
    }

    val connection = withContext(Dispatchers.IO) { factory.newConnection() }
    val queueName = "numbers_queue"

    // Determine number of consumers based on available processors
    val consumerCount = Runtime.getRuntime().availableProcessors()
    println("Starting $consumerCount consumers on IO dispatcher")

    // Create consumers
    val consumers = List(consumerCount) { consumerId ->
        val channel = withContext(Dispatchers.IO) { connection.createChannel() }
        CoroutineRabbitMQConsumer(this, channel, consumerId)
    }

    // Launch all consumers
    val consumerJobs = consumers.map { consumer ->
        launch(Dispatchers.IO) {
            consumer.startConsuming(queueName)
        }
    }

    println("All consumers started. Press Ctrl+C to exit.")

    // Handle graceful shutdown
    val shutdownChannel = kotlinx.coroutines.channels.Channel<Unit>()
    Runtime.getRuntime().addShutdownHook(Thread {
        println("\nShutting down consumers...")
        consumers.forEach { it.stop() }
        runBlocking { shutdownChannel.send(Unit) }
    })

    // Wait for shutdown signal
    try {
        shutdownChannel.receive()
        consumerJobs.forEach { it.cancel() }
        withContext(Dispatchers.IO) {
            connection.close()
        }
    } catch (e: Exception) {
        println("Shutdown completed")
    }
}
