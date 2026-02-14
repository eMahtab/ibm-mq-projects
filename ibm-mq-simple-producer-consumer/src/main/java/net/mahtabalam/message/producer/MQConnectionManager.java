package net.mahtabalam.message.producer;

import com.ibm.mq.jms.MQQueueConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import java.lang.IllegalStateException;


public class MQConnectionManager {

    private final String host;
    private final int port;
    private final String channel;
    private final String queueManager;
    private Connection connection;
    private Session session;

    public MQConnectionManager(String host, int port, String channel, String queueManager) {
        this.host = host;
        this.port = port;
        this.channel = channel;
        this.queueManager = queueManager;
    }

    public void connect() throws JMSException {
        System.out.println("=========================================");
        System.out.println("Connecting to IBM MQ");
        System.out.println("=========================================");
        System.out.println("Queue Manager: " + queueManager);
        System.out.println("Host: " + host + ":" + port);
        System.out.println("Channel: " + channel);
        System.out.println("=========================================\n");

        MQQueueConnectionFactory cf = createConnectionFactory();
        System.out.println("Establishing connection...");
        connection = cf.createConnection();
        connection.start();

        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        System.out.println("✓ Connected successfully!\n");
    }


    private MQQueueConnectionFactory createConnectionFactory() throws JMSException {
        MQQueueConnectionFactory cf = new MQQueueConnectionFactory();
        cf.setHostName(host);
        cf.setPort(port);
        cf.setChannel(channel);
        cf.setQueueManager(queueManager);
        cf.setTransportType(WMQConstants.WMQ_CM_CLIENT);
        cf.setCCSID(1208); // UTF-8 encoding
        return cf;
    }

    public Session getSession() {
        if (session == null) {
            throw new IllegalStateException("Not connected. Call connect() first.");
        }
        return session;
    }

    public Connection getConnection() {
        if (connection == null) {
            throw new IllegalStateException("Not connected. Call connect() first.");
        }
        return connection;
    }

    public boolean isConnected() {
        return connection != null && session != null;
    }

    public void disconnect() {
        try {
            if (session != null) {
                session.close();
                session = null;
            }
            if (connection != null) {
                connection.close();
                connection = null;
                System.out.println("Connection closed gracefully.");
            }
        } catch (JMSException e) {
            System.err.println("Error while closing connection:");
            e.printStackTrace();
        }
    }
}