package com.opencloud.example;
import java.lang.ref.WeakReference;
import java.lang.ref.ReferenceQueue;
/**
 * Dummy connection interface.
 * This represents some sort of underlying connection - the details don't
 * really matter.
 */
public class ConnectionImpl implements Connection {
    /**
     * Test to see if this connection is still valid.
     *
     * @return true iff this connection is still valid.
     */
	public boolean testConnection() {
    	return true;
    }
}
