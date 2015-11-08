package com.opencloud.example;

import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Collection;

public class Main {
	public static void main(String[] args) {
		Main main = new Main();

		System.out.println("Making a new pool");

		//System.out.println("Attempting simple run");

		//main.doSimpleRun();

		System.out.println("Attempting multithreaded run");

		main.doMultithreadedRun();
	}

	private void doSimpleRun() {
		//use a connection pool with 2 max connections and play around with it
		long timer; 
		ConnectionPool pool = new ConnectionPoolImpl(getFakeFactory(), 3);
		
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
	}

	private void doMultithreadedRun() {
		final ConnectionPool pool = new ConnectionPoolImpl(getFakeFactory(), 1);
		Collection<Thread> threads = new ArrayList<Thread>();

		for (int i = 0; i < 20; i++) {
			final int threadId = i;
			Thread thread = new Thread(new Runnable() {
				@Override
				public void run() {
					Main.doRun(threadId, pool);
				}
			});
			threads.add(thread);
			thread.start();
		}

		for (Thread t : threads) {
			try {
				t.join();
			}catch (InterruptedException e) {
				System.out.println("The impossible has happened. It's all over. You can go home.");
				System.exit(1);
			}
		}

		System.out.println("We made it! The program did not lock up");
	}

	public static void doRun(int threadId, ConnectionPool pool) {
		int runs = 50;
		System.out.println(threadId+": starting run");
		while (runs > 0) {
			Connection connection = pool.getConnection(100, TimeUnit.MILLISECONDS);
			if (connection == null) {
				System.out.println(threadId + ": failed to get a pooled connection.");
			}
			else if (!connection.testConnection()) {
				System.out.println(threadId + ": got connection but it expired.");
			}else{
				System.out.println(threadId + ": success! Got connection "+connection);
			}
			int randomVar = (int)System.currentTimeMillis() % 3;
			switch (randomVar) {
				case 0:
					connection = null; //let the connection be gc'd
					break;
				case 1:
					pool.releaseConnection(connection); //let the connection be gc'd
					break;
				default:
					System.out.println(threadId + ": Trying to garbage collect");
					System.gc();
			}
			runs--;
		}
	}

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