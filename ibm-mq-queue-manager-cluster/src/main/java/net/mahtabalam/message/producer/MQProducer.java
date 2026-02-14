package net.mahtabalam.message.producer;

import javax.jms.JMSException;

public class MQProducer {

    private static final String HOST = "localhost";
    private static final int PORT = 1416;
    private static final String CHANNEL = "SYSTEM.DEF.SVRCONN";
    private static final String CLUSTER_QMGR = "QMNGR2";  // Any cluster queue manager
    private static final String CLUSTER_QUEUE_NAME = "MY.APP.QUEUE";  // Cluster queue

    public static void main(String[] args) {
        MQConnectionManager connectionManager = null;
        MQMessageSender messageSender = null;

        try {
            // Step 1: Create connection manager with cluster-aware configuration
            connectionManager = new MQConnectionManager(HOST, PORT, CHANNEL, CLUSTER_QMGR);
            // Step 2: Connect to IBM MQ (can connect to any cluster member)
            connectionManager.connect();
            // Step 3: Create message sender for cluster queue
            messageSender = new MQMessageSender(connectionManager, CLUSTER_QUEUE_NAME);
            // Step 4: Initialize message sender
            messageSender.initialize();
            // Step 5: Send 100 messages (automatically distributed across cluster)
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
        System.err.println("1. Verify at least one cluster queue manager is running");
        System.err.println("2. Check if cluster queue '" + CLUSTER_QUEUE_NAME + "' exists");
        System.err.println("3. Confirm cluster queue is properly defined:");
        System.err.println("   DEFINE QLOCAL('" + CLUSTER_QUEUE_NAME + "') CLUSTER('MY.CLUSTER')");
        System.err.println("4. Verify cluster is active:");
        System.err.println("   DISPLAY CLUSQMGR(*) ALL");
        System.err.println("5. Ensure MQ listener is active on port " + PORT);
        System.err.println("6. Check cluster channel status:");
        System.err.println("   DISPLAY CHSTATUS(*) WHERE(CHLTYPE EQ CLUSSDR)");
        System.err.println("=========================================\n");
    }
}