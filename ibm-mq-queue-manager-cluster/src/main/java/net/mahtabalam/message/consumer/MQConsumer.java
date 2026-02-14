package net.mahtabalam.message.consumer;

import javax.jms.JMSException;

/**
 * IBM MQ Consumer with Asynchronous Message Listener
 * Continuously listens for and consumes messages from the queue
 */
public class MQConsumer {

    private static final String HOST = "localhost";
    private static final int PORT = 1415;
    private static final String CHANNEL = "SYSTEM.DEF.SVRCONN";
    private static final String CLUSTER_QMGR = "QMNGR1";  // Any cluster queue manager
    private static final String CLUSTER_QUEUE_NAME = "MY.APP.QUEUE";  // Cluster queue

    public static void main(String[] args) {
        MQConnectionManager connectionManager = null;
        MQMessageListener messageListener = null;

        try {
            System.out.println("\n╔═════════════════════════════════════════╗");
            System.out.println("║   IBM MQ ASYNCHRONOUS MESSAGE LISTENER  ║");
            System.out.println("╚═════════════════════════════════════════╝\n");

            // Step 1: Create connection manager
            connectionManager = new MQConnectionManager(HOST, PORT, CHANNEL, CLUSTER_QMGR);
            // Step 2: Connect to IBM MQ
            connectionManager.connect();
            // Step 3: Create and initialize message listener
            messageListener = new MQMessageListener(CLUSTER_QUEUE_NAME);
            messageListener.initialize(connectionManager);

            // Step 4: Add shutdown hook for graceful shutdown
            final MQConnectionManager finalConnectionManager = connectionManager;
            final MQMessageListener finalMessageListener = messageListener;

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\n\n╔═════════════════════════════════════════╗");
                System.out.println("║        SHUTDOWN SIGNAL RECEIVED         ║");
                System.out.println("╚═════════════════════════════════════════╝");
                System.out.println("\nTotal messages processed: " + finalMessageListener.getMessageCount());

                // Clean up resources
                if (finalMessageListener != null) {
                    finalMessageListener.close();
                }
                if (finalConnectionManager != null) {
                    finalConnectionManager.disconnect();
                }

                System.out.println("\n✓ Application terminated gracefully\n");
            }));

            // Step 5: Keep the application running
            // The listener will automatically process messages as they arrive
            keepAlive();

        } catch (JMSException e) {
            handleError(e);

        } finally {
            // This will only be reached if an exception occurs during initialization
            // Normal shutdown is handled by the shutdown hook
            if (messageListener != null && !Thread.currentThread().isInterrupted()) {
                messageListener.close();
            }
            if (connectionManager != null && !Thread.currentThread().isInterrupted()) {
                connectionManager.disconnect();
            }
        }
    }

    /**
     * Keep the application alive to continue listening for messages
     */
    private static void keepAlive() {
        try {
            // Keep the main thread alive
            // The JMS connection runs in separate threads
            while (true) {
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            System.out.println("\nApplication interrupted");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Handle JMS exceptions with detailed error information
     */
    private static void handleError(JMSException e) {
        System.err.println("\n╔═════════════════════════════════════════╗");
        System.err.println("║           ERROR OCCURRED                ║");
        System.err.println("╚═════════════════════════════════════════╝");
        System.err.println("\nError: " + e.getMessage());
        System.err.println("Error Code: " + (e.getErrorCode() != null ? e.getErrorCode() : "N/A"));

        if (e.getLinkedException() != null) {
            System.err.println("\nLinked Exception Details:");
            e.getLinkedException().printStackTrace();
        }

        System.err.println("\n=========================================");
        System.err.println("TROUBLESHOOTING TIPS:");
        System.err.println("=========================================");
        System.err.println("1. Verify Queue Manager '" + CLUSTER_QMGR + "' is running");
        System.err.println("   Command: dspmq");
        System.err.println("\n2. Check if queue '" + CLUSTER_QUEUE_NAME + "' exists");
        System.err.println("   Command: echo \"DISPLAY QUEUE(" + CLUSTER_QUEUE_NAME + ")\" | runmqsc " + CLUSTER_QMGR);
        System.err.println("\n3. Confirm channel '" + CHANNEL + "' is configured");
        System.err.println("   Command: echo \"DISPLAY CHANNEL(" + CHANNEL + ")\" | runmqsc " + CLUSTER_QMGR);
        System.err.println("\n4. Ensure MQ listener is active on port " + PORT);
        System.err.println("   Command: echo \"DISPLAY LISTENER(*)\" | runmqsc " + CLUSTER_QMGR);
        System.err.println("\n5. Check authentication/authorization settings");
        System.err.println("   To disable channel auth (dev only):");
        System.err.println("   echo \"ALTER QMGR CHLAUTH(DISABLED)\" | runmqsc " + CLUSTER_QMGR);
        System.err.println("   echo \"REFRESH SECURITY TYPE(CONNAUTH)\" | runmqsc " + CLUSTER_QMGR);
        System.err.println("\n6. Verify GET permissions on the queue");
        System.err.println("   Command: echo \"DISPLAY QSTATUS(" + CLUSTER_QUEUE_NAME + ")\" | runmqsc " + CLUSTER_QMGR);
        System.err.println("\n7. Check MQ error logs");
        System.err.println("   Location: /var/mqm/qmgrs/" + CLUSTER_QMGR + "/errors/AMQERR01.LOG");
        System.err.println("=========================================\n");
    }
}
