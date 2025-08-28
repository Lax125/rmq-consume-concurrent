package org.example

import com.rabbitmq.client.ConnectionFactory

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
fun main() {
    val factory = ConnectionFactory().apply {
        host = "localhost"
        username = "guest"
        password = "guest"
    }

    factory.newConnection().use { connection ->
        connection.createChannel().use { channel ->
            val queueName = "numbers_queue"

            // Create queue (durable to survive broker restarts)
            channel.queueDeclare(queueName, true, false, false, null)

            // Send messages 1 to 100
            for (i in 1..100) {
                val message = i.toString()
                channel.basicPublish("", queueName, null, message.toByteArray())
                println("Sent: $message")
            }

            println("All messages sent!")
        }
    }
}
