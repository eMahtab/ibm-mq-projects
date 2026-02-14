package net.mahtabalam.message.consumer;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Message;
import javax.jms.TextMessage;
import java.lang.IllegalStateException;

public class MQMessageReceiver {

    private final MQConnectionManager connectionManager;
    private final String queueName;
    private MessageConsumer messageConsumer;

    public MQMessageReceiver(MQConnectionManager connectionManager, String queueName) {
        this.connectionManager = connectionManager;
        this.queueName = queueName;
    }

    public void initialize() throws JMSException {
        if (!connectionManager.isConnected()) {
            throw new IllegalStateException("Connection manager is not connected.");
        }

        Session session = connectionManager.getSession();
        Queue queue = session.createQueue(queueName);

        messageConsumer = session.createConsumer(queue);

        System.out.println("✓ Message receiver initialized for queue: " + queueName + "\n");
    }

    /**
     * Receive messages from the queue until timeout or no more messages
     */
    public void receiveMessages(int timeoutMs) throws JMSException {
        if (messageConsumer == null) {
            throw new IllegalStateException("Message receiver not initialized. Call initialize() first.");
        }

        System.out.println("Starting to receive messages from queue: " + queueName);
        System.out.println("Timeout: " + timeoutMs + " ms\n");
        long startTime = System.currentTimeMillis();
        int messageCount = 0;

        while (true) {
            Message message = messageConsumer.receive(timeoutMs);
            if (message == null) {
                System.out.println("\nNo more messages available (timeout reached).");
                break;
            }
            messageCount++;
            processMessage(message, messageCount);

            if (messageCount % 10 == 0) {
                System.out.println("  Received " + messageCount + " messages...");
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        printSummary(messageCount, duration);
    }

    /**
     * Receive a specific number of messages
     */
    public void receiveMessages(int messageCount, int timeoutMs) throws JMSException {
        if (messageConsumer == null) {
            throw new IllegalStateException("Message receiver not initialized. Call initialize() first.");
        }

        System.out.println("Receiving up to " + messageCount + " messages from queue: " + queueName);
        System.out.println("Timeout per message: " + timeoutMs + " ms\n");

        long startTime = System.currentTimeMillis();
        int receivedCount = 0;

        for (int i = 1; i <= messageCount; i++) {
            Message message = messageConsumer.receive(timeoutMs);

            if (message == null) {
                System.out.println("\nNo more messages available after " + receivedCount + " messages.");
                break;
            }

            receivedCount++;
            processMessage(message, receivedCount);

            if (receivedCount % 10 == 0) {
                System.out.println("  Received " + receivedCount + " messages...");
            }
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        printSummary(receivedCount, duration);
    }

    private void processMessage(Message message, int messageNumber) throws JMSException {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String text = textMessage.getText();

            // Extract message properties
            int msgNumber = message.propertyExists("MessageNumber")
                    ? message.getIntProperty("MessageNumber") : -1;
            String msgType = message.propertyExists("MessageType")
                    ? message.getStringProperty("MessageType") : "UNKNOWN";
            String qName = message.propertyExists("QueueName")
                    ? message.getStringProperty("QueueName") : "UNKNOWN";

            // Display received message details
            System.out.println("─────────────────────────────────────────");
            System.out.println("Message #" + messageNumber + " received:");
            System.out.println("  Content: " + text);
            System.out.println("  Message Number: " + msgNumber);
            System.out.println("  Message Type: " + msgType);
            System.out.println("  Queue Name: " + qName);
            System.out.println("  JMS Message ID: " + message.getJMSMessageID());
            System.out.println("  JMS Timestamp: " + message.getJMSTimestamp());
            System.out.println("─────────────────────────────────────────\n");

        } else {
            System.out.println("Received non-text message: " + message.getClass().getName());
        }
    }

    private void printSummary(int messageCount, long duration) {
        System.out.println("\n=========================================");
        System.out.println("✓ RECEIVING COMPLETE!");
        System.out.println("=========================================");
        System.out.println("Queue: " + queueName);
        System.out.println("Total messages received: " + messageCount);
        System.out.println("Time taken: " + duration + " ms");
        if (messageCount > 0) {
            System.out.println("Average: " + String.format("%.2f", duration / (double) messageCount) + " ms per message");
        }
        System.out.println("=========================================\n");
    }

    /**
     * Receive messages using a MessageListener (asynchronous)
     */
    public void receiveMessagesAsync() throws JMSException {
        if (messageConsumer == null) {
            throw new IllegalStateException("Message receiver not initialized. Call initialize() first.");
        }

        System.out.println("Setting up asynchronous message listener for queue: " + queueName + "\n");

        final int[] messageCount = {0};
        final long startTime = System.currentTimeMillis();

        messageConsumer.setMessageListener(message -> {
            try {
                messageCount[0]++;
                processMessage(message, messageCount[0]);
            } catch (JMSException e) {
                System.err.println("Error processing message: " + e.getMessage());
                e.printStackTrace();
            }
        });

        System.out.println("✓ Async listener activated. Messages will be processed as they arrive.");
        System.out.println("Press Ctrl+C to stop...\n");
    }

    public void close() {
        try {
            if (messageConsumer != null) {
                messageConsumer.close();
                messageConsumer = null;
                System.out.println("Message receiver closed.");
            }
        } catch (JMSException e) {
            System.err.println("Error closing message receiver:");
            e.printStackTrace();
        }
    }

    public boolean isInitialized() {
        return messageConsumer != null;
    }

    public String getQueueName() {
        return queueName;
    }
}