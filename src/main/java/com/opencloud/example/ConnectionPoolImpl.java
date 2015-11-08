package com.opencloud.example;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.*;
import java.util.*;
import java.lang.ref.*;
import java.io.PrintStream;
/**
 * Connection pool implementation.
 *
 * This implementation should:
 *
 * - Use the provided ConnectionFactory implementation to build new
 *   Connection objects.
 * - Allow up to {@code maxConnections} simultaneous connections
 *   (both in-use and idle)
 * - Call Connection.testConnection() before returning a Connection to a
 *   caller; if testConnection() returns false, this Connection
 *   instance should be discarded and a different Connection obtained.
 * - Be safe to use by multiple callers simultaneously from different threads
 *
 * You may find the locking and queuing objects provided by
 * java.util.concurrent useful.
 *
 * Some possible extensions:
 *
 * - Check that connections returned via releaseConnection() were actually
 *   allocated via getConnection() (and haven't already been returned)
 * - Test idle connections periodically, and discard those which fail a
 *   testConnection() check.
 * - Detect Connections that have been handed out to a caller, but where the
 *   caller has discarded the Connection object, and don't count them as
 *   "in use". (hint: have the pool store WeakReferences to in-use connections,
 *   and use that to detect when they become only weakly reachable)
 *
 */
public class ConnectionPoolImpl implements ConnectionPool {
    /**
     * Construct a new pool that uses a provided factory to construct
     * connections, and allows a given maximum number of connections 
     * simultaneously.
     *
     * @param factory the factory to use to construct connections
     * @param maxConnections the number of simultaneous connections to allow
     */

    private LinkedBlockingQueue<Connection> idleConnections = new LinkedBlockingQueue<Connection>();
    private ReferenceQueue<Connection> abandonedConnections = new ReferenceQueue<Connection>();

    private ConnectionFactory factory;
    private int maxConnections;
    private int connectionsInExistence = 0;

    private PrintStream log = System.out;

    private ArrayList<WeakReference<Connection>> inUseConnections = new ArrayList<WeakReference<Connection>>();

    public ConnectionPoolImpl(ConnectionFactory factory,
                              int maxConnections)
    {
        this.factory = factory;
        this.maxConnections = maxConnections;
    }

    public synchronized Connection getConnection(long delay, TimeUnit units)
    {
        Connection connection = tryCreateNewConnection();

        if (connection == null) {
            log("Maximum connection limit reached. Attempting to get GC'd connection");
            connection = tryGetGarbagedConnection();
        }

        if (connection == null) {
            log("No GC'd connections. Attempting to load connection from queue");
            connection = tryGetQueuedConnection(delay, units);
        }

        log("Exiting connection pool method. Idle connections "+idleConnections.size() + ", Current connections: "+connectionsInExistence);

        return connection;
    }

    private synchronized Connection tryCreateNewConnection() {
        if (connectionsInExistence < maxConnections) {
            Connection connection;
            try {
                connection = factory.newConnection();
            }catch (ConnectionException e) {
                log("Connection exception occurred. Could not create connection.", e);
                return null;
            }
            inUseConnections.add(new WeakReference(connection, abandonedConnections));
            connectionsInExistence++;
            log("Created connection. There are now "+connectionsInExistence +" connections");
            return connection;
        }else{
            log("Cannot create new connection. The limit is "+maxConnections+" and there are currently "+connectionsInExistence +" connections");
            return null;
        }
    }

    private synchronized Connection tryGetQueuedConnection(long delay, TimeUnit units) {
        try {
            return idleConnections.poll(delay, units);
        }catch (InterruptedException e) {
            log("Interrupted while getting queued connection");
            return null;
        }
    }

    private synchronized Connection tryGetGarbagedConnection() {
        if (abandonedConnections.poll() != null) {
            log("A connection has been GC'd. A new connection will be created");
            try {
                abandonedConnections.remove(1);
            }catch (InterruptedException e) {
                return null;
            }
            connectionsInExistence--;
            return tryCreateNewConnection();
        }else{
            return null;
        }
    }
        
    public synchronized void releaseConnection(Connection connection)
    {
        if (connection == null) {
            log("User releasing null connection");
            return;
        }else if (idleConnections.contains(connection)) {
            log("User releasing connection that is already idle");
            return;
        }

        int indexOfOurConnection = -1, i = 0;
        log("Checking whether this connection owned by the pool");
        for (WeakReference<Connection> connectionRef : inUseConnections) {
            if (connection.equals(connectionRef.get())) {
                indexOfOurConnection = i;
                break;
            }
            i++;
        }

        if (indexOfOurConnection == -1) {
            log("Connection is NOT part of pool");
            return;
        }else{
            log("Connection is part of pool");
            inUseConnections.remove(i);
        }

        if (connection.testConnection()) {
            idleConnections.offer(connection);
            log("Returned connection being recycled");
        }else{
            connectionsInExistence = connectionsInExistence - 1;
            log("Returned connection is dead");
        }
    }

    private void log(Object... logParams) {
        for (Object obj : logParams) {
            log.print(obj);
            log.print(" ");
        }
        log.print("\n");
    }
}
