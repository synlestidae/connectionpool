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
    private int connectionsOutsidePool = 0;

    private PrintStream log = System.out;

    private ArrayList<WeakReference<Connection>> inUseConnections = new ArrayList<WeakReference<Connection>>();

    public ConnectionPoolImpl(ConnectionFactory factory,
                              int maxConnections)
    {
        this.factory = factory;
        this.maxConnections = maxConnections;
    }

    /**
     * Retrieves a connection from the pool. Runs through the following steps,
     * in order, to reliably retrieve a connection:
     * 1. Check if a new connection can be created
     * 2. Check for a connection that has already been returned
     * 3. Check if a connection has been garbage-collected.
     * 4. Wait for a connection to be returned
     * 
     * If any step results in an active connection, it will be returned. If no step 
     * procures a connection, null is returned.
    */
    public Connection getConnection(long delay, TimeUnit units)
    {
        Connection connection = null;

        synchronized (this) {
            if (idleConnections.size() > 0) {
                log("Checking if a connection has been returned");
                long[] delayArr = new long[]{delay};
                connection = tryGetQueuedConnectionNonBlocking(delayArr, units);
                delay = delayArr[0];
            }
        }

        if (connection == null) {
            connection = tryCreateNewConnection();
        }

        if (connection == null) {
            log("Maximum connection limit reached. Attempting to check for GC'd connection");
            connection = tryGetGarbagedConnection();
        }

        if (connection == null) {
            log("No GC'd connections. Attempting to wait for connection from queue");
            connection = tryGetQueuedConnection(delay, units);
        }

        synchronized (this) {
            log("Exiting connection pool method. Idle connections "+idleConnections.size() + 
                ", External connections: "+inUseConnections.size());
        }
        return connection;
    }

    /**
    * Creates and returns a new connection if the connection limit has NOT
    * been reached. Otherwise returns null.
    */
    private synchronized Connection tryCreateNewConnection() {
        if (inUseConnections.size() + idleConnections.size() < maxConnections) {
            Connection connection;
            try {
                connection = factory.newConnection();
            }catch (ConnectionException e) {
                log("Connection exception occurred. Could not create connection.", e);
                return null;
            }
            inUseConnections.add(new WeakReference(connection, abandonedConnections));
            log("Created connection. There are now "+(inUseConnections.size() + idleConnections.size()) +" connections");
            return connection;
        }else{
            log("Cannot create new connection. The limit is "+maxConnections+" and there are currently "+(inUseConnections.size() + idleConnections.size()) +" connections");
            return null;
        }
    }

    /**
    * Wait for a connection to become available on the queue and return it,
    * or return null if nothing is available.
    */
    private Connection tryGetQueuedConnection(long delay, TimeUnit units) {
        try {
            Connection connection = idleConnections.poll(delay, units);
            if (connection != null && !connection.testConnection()) {
                //at this point the delay may not have expired. 
                //would be nice to try again with a shorter delay!
                return null;
            }
            
            if (connection != null) {
                inUseConnections.add(new WeakReference(connection, abandonedConnections));
                log("Got gueued connection");
            }else{
                log("Could not get queued connection");
            }
            return connection;
        }catch (InterruptedException e) {
            log("Interrupted while getting queued connection");
            return null;
        }
    }

    /**
    * Return a connection from queue if one is there, otherwise null.
    * Will discard connections. This method doesn't wait for a connection 
    * to be placed on the queue. It DOES re-poll the queue if the 
    * connection it gets back is not active. To prevent this looping indefinitely,
    * time limit parameters are used.
    */
    private synchronized Connection tryGetQueuedConnectionNonBlocking(long[] delayArr, TimeUnit units) {
        long delay = delayArr[0];

        long timeA = System.currentTimeMillis();
        Connection connection = idleConnections.poll();
        long timeB = System.currentTimeMillis();

        while (connection != null && !connection.testConnection()
            && units.convert(timeB - timeA, TimeUnit.MILLISECONDS) <= delay) {
            connection = idleConnections.poll();
            timeB = System.currentTimeMillis();
        }

        if (connection != null) {
            inUseConnections.add(new WeakReference(connection, abandonedConnections));
            log("Got gueued connection");
        }else{
            log("Could not get queued connection");
            }

        delayArr[0] = delay;

        return connection;
    }

    /**
    * Retrieve a GC'd connection and discard it, creating a new one in its
    * place. An alternative implementation could re-use the connection --
    * this method book-keeps, instead of recycling. 
    */
    private synchronized Connection tryGetGarbagedConnection() {
        log("A connection has been GC'd. A new connection will be created in its place");
            Reference<? extends Connection> connectionRef = abandonedConnections.poll();
            
            if (connectionRef != null && connectionRef.get() == null) {
                int i = 0, index = -1;
                for (Reference<? extends Connection> ref : inUseConnections) {
                    if (ref.get() == null) {
                        index = i;
                        break;
                    }
                }
                if (index >= 0) {
                    inUseConnections.remove(index);
                    log("Removed null ref from inUseConnections");
                }else{
                    log("WARNING! Couldn't remove null GC'd connection from inUseConnections");
                }
            }
            else if (connectionRef != null) {
                if (!inUseConnections.remove(connectionRef)) {
                    log("WARNING! Couldn't remove GC'd connection from inUseConnections");
                    return null;
                }else{
                    log("Removed connection ref");
                }
            }else if (connectionRef == null) {
                log("No GC'd connection available");
                return null;
            }
        return tryCreateNewConnection();
    }
        
    public void releaseConnection(Connection connection)
    {
        try {
            log("User releasing a connection");
            if (connection == null) {
                log("User releasing null connection");
                return;
            }else if (idleConnections.contains(connection)) {
                log("User releasing connection that is already idle");
                return;
            }

            removeInUseConnection(connection);

            if (connection.testConnection()) {
                idleConnections.offer(connection);
                log("Returned connection being recycled");
            }else{
                log("Returned connection is dead. Removing it from in use connection.");
            }
        }finally {
            log("Exiting connection pool release method. Idle connections "+idleConnections.size() + 
                ", External connections: "+inUseConnections.size());
        }
    }

    private boolean removeInUseConnection(Connection connection) {
        log("Removing an in-use connection");
        int indexOfOurConnection = -1;
        int i = 0;

        if (connection == null) {
            log("WARNING! Attempt to remove null connection from references");
            return false;
        }

        for (WeakReference<Connection> connectionRef : inUseConnections) {
            if ((connection == null && connectionRef.get() == null)
                || connection.equals(connectionRef.get())) {
                indexOfOurConnection = i;
                break;
            }
            i++;
        }

        if (indexOfOurConnection == -1) {
            log("Connection is NOT part of pool");
            return false;
        }else{
            log("Connection is part of pool");
            inUseConnections.remove(i);
            return true;
        }
    }

    private void log(Object... logParams) {
        if (maxConnections > 0) return; //used to stop log

        for (Object obj : logParams) {
            log.print(obj);
            log.print(" ");
        }
        log.print("\n");
    }
}
