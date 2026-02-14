package net.mahtabalam.message.producer;

import javax.jms.*;
import java.lang.IllegalStateException;

public class MQMessageSender {

    private final MQConnectionManager connectionManager;
    private final String queueName;
    private MessageProducer messageProducer;

    public MQMessageSender(MQConnectionManager connectionManager, String queueName) {
        this.connectionManager = connectionManager;
        this.queueName = queueName;
    }

    public void initialize() throws JMSException {
        if (!connectionManager.isConnected()) {
            throw new IllegalStateException("Connection manager is not connected.");
        }

        Session session = connectionManager.getSession();
        Queue queue = session.createQueue(queueName);

        messageProducer = session.createProducer(queue);
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

        System.out.println("✓ Message sender initialized for queue: " + queueName + "\n");
    }

    public void sendMessages(int messageCount) throws JMSException {
        if (messageProducer == null) {
            throw new IllegalStateException("Message sender not initialized. Call initialize() first.");
        }

        System.out.println("Sending " + messageCount + " messages to queue: " + queueName + "\n");
        long startTime = System.currentTimeMillis();

        for (int i = 1; i <= messageCount; i++) {
            sendMessage(i);
            if (i % 10 == 0) {
                System.out.println("  Sent " + i + " messages...");
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        printSummary(messageCount, duration);
    }

    private void sendMessage(int messageNumber) throws JMSException {
        Session session = connectionManager.getSession();
        TextMessage message = session.createTextMessage();

        String messageText = String.format(
                "Message #%d | Timestamp: %d | Status: Delivered",
                messageNumber, System.currentTimeMillis()
        );
        message.setText(messageText);

        message.setIntProperty("MessageNumber", messageNumber);
        message.setStringProperty("MessageType", "TEST");
        message.setStringProperty("QueueName", queueName);

        messageProducer.send(message);
    }

    private void printSummary(int messageCount, long duration) {
        System.out.println("\n=========================================");
        System.out.println("✓ SUCCESS!");
        System.out.println("=========================================");
        System.out.println("Queue: " + queueName);
        System.out.println("Total messages sent: " + messageCount);
        System.out.println("Time taken: " + duration + " ms");
        System.out.println("Average: " + String.format("%.2f", duration / (double) messageCount) + " ms per message");
        System.out.println("=========================================\n");
    }

    public void close() {
        try {
            if (messageProducer != null) {
                messageProducer.close();
                messageProducer = null;
                System.out.println("Message sender closed.");
            }
        } catch (JMSException e) {
            System.err.println("Error closing message sender:");
            e.printStackTrace();
        }
    }

    public boolean isInitialized() {
        return messageProducer != null;
    }

    public String getQueueName() {
        return queueName;
    }
}