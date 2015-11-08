package com.opencloud.example;

import java.util.concurrent.TimeUnit;

public class Main {
	public static void main(String[] args) {
		Main main = new Main();

		System.out.println("Making a new pool");

		ConnectionPool pool = new ConnectionPoolImpl(getFakeFactory(), 3);

		System.out.println("Attempting run-through #1");

		main.doSimpleRunthrough1(pool);

		System.gc();

		System.out.println("Attempting run-through #2");

		//main.doSimpleRunthrough2(pool);
	}

	private void doSimpleRunthrough1(ConnectionPool pool) {
		//use a connection pool with 2 max connections and play around with it
		long timer;
		
		System.out.println("Getting first connection");

		timer = System.currentTimeMillis();
		Connection connection1 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 1 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection1 != null;

		timer = System.currentTimeMillis();
		Connection connection2 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 2 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection2 != null;

		timer = System.currentTimeMillis();
		Connection connection3 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 3 took "+(System.currentTimeMillis() - timer)+" milliseconds");


		System.gc();

		assert connection3 != null;	

		timer = System.currentTimeMillis();
		Connection connection4 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 4 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection4 == null;	

		timer = System.currentTimeMillis();
		Connection connection5 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 5 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection5 == null;	

		System.out.print("Our connections are:");
		for (Connection connection : new Connection[] {connection1, 
			connection2, connection3, connection4, connection5}) {
			System.out.print(" "+connection);
		}
		System.out.print("\n");
	}

	/*private void doSimpleRunthrough2(ConnectionPool pool) {
		//use a connection pool with 2 max connections and play around with it
		long timer;
		
		System.out.println("Getting first connection");

		timer = System.currentTimeMillis();
		Connection connection1 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 1 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection1 != null;

		timer = System.currentTimeMillis();
		Connection connection2 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 2 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection2 != null;

		timer = System.currentTimeMillis();
		Connection connection3 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 3 took "+(System.currentTimeMillis() - timer)+" milliseconds");


		System.gc();

		assert connection3 != null;	

		pool.releaseConnection(connection3);

		timer = System.currentTimeMillis();
		Connection connection4 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 4 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection4 != null;	

		timer = System.currentTimeMillis();
		Connection connection5 = pool.getConnection(100, TimeUnit.MILLISECONDS);

		System.out.println("Connection 5 took "+(System.currentTimeMillis() - timer)+" milliseconds");

		assert connection5 == null;	

		System.out.print("Our connections are:");
		for (Connection connection : new Connection[] {connection1, 
			connection2, connection3, connection4, connection5}) {
			System.out.print(" "+connection);
		}
		System.out.print("\n");
	}*/

	private static ConnectionFactory getFakeFactory() {
		return new ConnectionFactory() {
			@Override
			public Connection newConnection() {
				return new Connection() {
					@Override
					public boolean testConnection() {
						return true;
					}
				};
			}
		};
	}
}