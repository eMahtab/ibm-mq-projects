package net.mahtabalam.message.consumer2;

import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

/**
 * Manages IBM MQ connections
 */
public class MQConnectionManager {

    private final String host;
    private final int port;
    private final String channel;
    private final String queueManager;

    private MQConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;

    public MQConnectionManager(String host, int port, String channel, String queueManager) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.queueManager = queueManager;
    }

    /**
     * Establishes connection to IBM MQ
     */
    public void connect() throws JMSException {
        System.out.println("\n=========================================");
        System.out.println("Connecting to IBM MQ...");
        System.out.println("=========================================");
        System.out.println("Host: " + host);
        System.out.println("Port: " + port);
        System.out.println("Channel: " + channel);
        System.out.println("Queue Manager: " + queueManager);
        System.out.println("=========================================\n");

        // Create connection factory
        connectionFactory = new MQConnectionFactory();
        connectionFactory.setHostName(host);
        connectionFactory.setPort(port);
        connectionFactory.setChannel(channel);
        connectionFactory.setQueueManager(queueManager);
        connectionFactory.setTransportType(WMQConstants.WMQ_CM_CLIENT);

        // Create connection
        connection = connectionFactory.createConnection();

        // Create session (non-transacted, auto-acknowledge)
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        // Start the connection (required for message listeners)
        connection.start();

        System.out.println("✓ Successfully connected to IBM MQ\n");
    }

    /**
     * Disconnects from IBM MQ
     */
    public void disconnect() {
        System.out.println("\nDisconnecting from IBM MQ...");

        try {
            if (session != null) {
                session.close();
                System.out.println("✓ Session closed");
            }
        } catch (JMSException e) {
            System.err.println("✗ Error closing session: " + e.getMessage());
        }

        try {
            if (connection != null) {
                connection.close();
                System.out.println("✓ Connection closed");
            }
        } catch (JMSException e) {
            System.err.println("✗ Error closing connection: " + e.getMessage());
        }

        System.out.println("=========================================\n");
    }

    public Session getSession() {
        return session;
    }

    public Connection getConnection() {
        return connection;
    }
}
