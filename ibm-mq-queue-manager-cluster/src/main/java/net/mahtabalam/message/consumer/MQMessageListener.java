package net.mahtabalam.message.consumer;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.BytesMessage;
import javax.jms.ObjectMessage;
import javax.jms.MapMessage;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Asynchronous message listener for IBM MQ
 * Listens and consumes messages as they arrive in the queue
 */
public class MQMessageListener implements MessageListener {

    private final String queueName;
    private final AtomicInteger messageCount;
    private final DateTimeFormatter formatter;
    private MessageConsumer consumer;

    public MQMessageListener(String queueName) {
        this.queueName = queueName;
        this.messageCount = new AtomicInteger(0);
        this.formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    }

    /**
     * Initialize the message listener
     */
    public void initialize(MQConnectionManager connectionManager) throws JMSException {
        System.out.println("=========================================");
        System.out.println("Initializing Message Listener");
        System.out.println("=========================================");
        System.out.println("Queue: " + queueName);
        System.out.println("=========================================\n");
        // Get the queue
        Queue queue = connectionManager.getSession().createQueue(queueName);
        // Create message consumer
        consumer = connectionManager.getSession().createConsumer(queue);
        // Set this as the message listener
        consumer.setMessageListener(this);
        System.out.println("✓ Message Listener initialized successfully");
        System.out.println("✓ Now listening for messages on queue: " + queueName);
        System.out.println("\n=========================================");
        System.out.println("WAITING FOR MESSAGES...");
        System.out.println("=========================================");
        System.out.println("Press Ctrl+C to stop listening\n");
    }

    /**
     * Called automatically when a message arrives
     */
    @Override
    public void onMessage(Message message) {
        int currentCount = messageCount.incrementAndGet();
        String timestamp = LocalDateTime.now().format(formatter);

        try {
            System.out.println("\n┌─────────────────────────────────────────");
            System.out.println("│ MESSAGE RECEIVED #" + currentCount);
            System.out.println("├─────────────────────────────────────────");
            System.out.println("│ Timestamp: " + timestamp);
            System.out.println("│ Message ID: " + message.getJMSMessageID());
            System.out.println("│ Correlation ID: " + (message.getJMSCorrelationID() != null ?
                    message.getJMSCorrelationID() : "N/A"));
            System.out.println("│ Priority: " + message.getJMSPriority());
            System.out.println("│ Delivery Mode: " + (message.getJMSDeliveryMode() == DeliveryMode.PERSISTENT ?
                    "PERSISTENT" : "NON_PERSISTENT"));
            System.out.println("├─────────────────────────────────────────");

            // Process based on message type
            if (message instanceof TextMessage) {
                processTextMessage((TextMessage) message);
            } else if (message instanceof BytesMessage) {
                processBytesMessage((BytesMessage) message);
            } else if (message instanceof ObjectMessage) {
                processObjectMessage((ObjectMessage) message);
            } else if (message instanceof MapMessage) {
                processMapMessage((MapMessage) message);
            } else {
                System.out.println("│ Message Type: " + message.getClass().getSimpleName());
                System.out.println("│ Content: [Unsupported message type]");
            }

            // Display message properties if any
            displayMessageProperties(message);

            System.out.println("└─────────────────────────────────────────");
            System.out.println("✓ Message #" + currentCount + " processed successfully\n");

        } catch (JMSException e) {
            System.err.println("\n✗ Error processing message #" + currentCount);
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Process TextMessage
     */
    private void processTextMessage(TextMessage textMessage) throws JMSException {
        String text = textMessage.getText();
        System.out.println("│ Message Type: TextMessage");
        System.out.println("│ Content Length: " + (text != null ? text.length() : 0) + " characters");
        System.out.println("├─────────────────────────────────────────");
        System.out.println("│ MESSAGE CONTENT:");
        System.out.println("├─────────────────────────────────────────");

        if (text != null && text.length() > 0) {
            // Split long messages into multiple lines
            String[] lines = text.split("\n");
            for (String line : lines) {
                if (line.length() <= 70) {
                    System.out.println("│ " + line);
                } else {
                    // Wrap long lines
                    int start = 0;
                    while (start < line.length()) {
                        int end = Math.min(start + 70, line.length());
                        System.out.println("│ " + line.substring(start, end));
                        start = end;
                    }
                }
            }
        } else {
            System.out.println("│ [Empty message]");
        }
    }

    /**
     * Process BytesMessage
     */
    private void processBytesMessage(BytesMessage bytesMessage) throws JMSException {
        long length = bytesMessage.getBodyLength();
        System.out.println("│ Message Type: BytesMessage");
        System.out.println("│ Content Length: " + length + " bytes");

        if (length > 0 && length < 1024) {
            byte[] data = new byte[(int) length];
            bytesMessage.readBytes(data);
            System.out.println("│ Content (hex): " + bytesToHex(data));
        }
    }

    /**
     * Process ObjectMessage
     */
    private void processObjectMessage(ObjectMessage objectMessage) throws JMSException {
        Object object = objectMessage.getObject();
        System.out.println("│ Message Type: ObjectMessage");
        System.out.println("│ Object Type: " + (object != null ? object.getClass().getName() : "null"));
        System.out.println("│ Content: " + object);
    }

    /**
     * Process MapMessage
     */
    private void processMapMessage(MapMessage mapMessage) throws JMSException {
        System.out.println("│ Message Type: MapMessage");
        System.out.println("│ Map Entries:");

        java.util.Enumeration<?> mapNames = mapMessage.getMapNames();
        while (mapNames.hasMoreElements()) {
            String name = (String) mapNames.nextElement();
            Object value = mapMessage.getObject(name);
            System.out.println("│   " + name + " = " + value);
        }
    }

    /**
     * Display custom message properties
     */
    private void displayMessageProperties(Message message) throws JMSException {
        java.util.Enumeration<?> propertyNames = message.getPropertyNames();

        if (propertyNames.hasMoreElements()) {
            System.out.println("├─────────────────────────────────────────");
            System.out.println("│ CUSTOM PROPERTIES:");

            while (propertyNames.hasMoreElements()) {
                String propertyName = (String) propertyNames.nextElement();
                Object propertyValue = message.getObjectProperty(propertyName);
                System.out.println("│   " + propertyName + " = " + propertyValue);
            }
        }
    }

    /**
     * Convert bytes to hex string
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < Math.min(bytes.length, 50); i++) {
            sb.append(String.format("%02X ", bytes[i]));
        }
        if (bytes.length > 50) {
            sb.append("...");
        }
        return sb.toString();
    }

    /**
     * Close the message consumer
     */
    public void close() {
        try {
            if (consumer != null) {
                consumer.close();
                System.out.println("✓ Message Listener closed");
            }
        } catch (JMSException e) {
            System.err.println("✗ Error closing message consumer: " + e.getMessage());
        }
    }

    /**
     * Get the total number of messages received
     */
    public int getMessageCount() {
        return messageCount.get();
    }
}