package net.mahtabalam.message.producer;

import javax.jms.JMSException;

public class MQProducer {

    private static final String HOST = "localhost";
    private static final int PORT = 1414;
    private static final String CHANNEL = "SYSTEM.DEF.SVRCONN";
    private static final String QMGR = "MY.TEST.QMNGR";
    private static final String QUEUE_NAME = "FIRST.TEST.QUEUE";

    public static void main(String[] args) {
        MQConnectionManager connectionManager = null;
        MQMessageSender messageSender = null;

        try {
            // Step 1: Create connection manager
            connectionManager = new MQConnectionManager(HOST, PORT, CHANNEL, QMGR);
            // Step 2: Connect to IBM MQ
            connectionManager.connect();
            // Step 3: Create message sender
            messageSender = new MQMessageSender(connectionManager, QUEUE_NAME);
            // Step 4: Initialize message sender
            messageSender.initialize();
            // Step 5: Send 100 messages
            messageSender.sendMessages(100);
        } catch (JMSException e) {
            handleError(e);

        } finally {
            // Clean up resources
            if (messageSender != null) {
                messageSender.close();
            }
            if (connectionManager != null) {
                connectionManager.disconnect();
            }
        }
    }


    private static void handleError(JMSException e) {
        System.err.println("\n=========================================");
        System.err.println("✗ ERROR OCCURRED");
        System.err.println("=========================================");
        System.err.println("Error: " + e.getMessage());
        System.err.println("Error Code: " + (e.getErrorCode() != null ? e.getErrorCode() : "N/A"));

        if (e.getLinkedException() != null) {
            System.err.println("\nLinked Exception Details:");
            e.getLinkedException().printStackTrace();
        }

        System.err.println("\nTroubleshooting Tips:");
        System.err.println("1. Verify Queue Manager '" + QMGR + "' is running");
        System.err.println("2. Check if queue '" + QUEUE_NAME + "' exists");
        System.err.println("3. Confirm channel '" + CHANNEL + "' is configured");
        System.err.println("4. Ensure MQ listener is active on port " + PORT);
        System.err.println("5. Check authentication/authorization settings");
        System.err.println("   Run: ALTER QMGR CHLAUTH(DISABLED)");
        System.err.println("=========================================\n");
    }
}