package com.couchbase.loadgen.client;

import java.util.LinkedList;
import java.util.List;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.DataStore;
import com.couchbase.loadgen.exception.DataStoreException;
import com.couchbase.loadgen.exception.UnknownDataStoreException;
import com.couchbase.loadgen.memcached.MemcachedFactory;
import com.couchbase.loadgen.workloads.Workload;

/**
 * A thread pool is a group of a limited number of threads that are used to
 * execute tasks.
 */
public class ClientThreadPool extends ThreadGroup {
	private static int threadPoolID;
	private List<PooledThread> threads;
	private boolean isAlive;
	private int threadID;
	private int ops;
	private int lastTarget;
	private long st;
	private long opsdone;
	

	public ClientThreadPool(int threadcount, Workload workload, String dbname) {
		super("ThreadPool-" + (threadPoolID++));
		this.threads = new LinkedList<PooledThread>();
		this.ops = ((Integer)Config.getConfig().get(Config.OP_COUNT)).intValue();
		this.lastTarget = ((Integer)Config.getConfig().get(Config.TARGET));
		this.opsdone = 0;
		this.st = System.currentTimeMillis();
		setDaemon(true);

		isAlive = true;
		
		for (int i = 0; i < threadcount; i++) {
			DataStore db = null;
			try {
				if (workload instanceof com.couchbase.loadgen.workloads.MemcachedCoreWorkload)
					db = MemcachedFactory.newMemcached(dbname);
				else {
					System.out.println("Invalid Database/Workload Combination");
					System.exit(0);
				}
				db.init();
			} catch (UnknownDataStoreException e) {
				System.out.println("Unknown DataStore " + dbname);
				System.exit(0);
			} catch (DataStoreException e) {
				e.printStackTrace();
				System.exit(0);
			}
			PooledThread thread = new PooledThread(workload, db);
			threads.add(thread);
			thread.start();
		}
	}

	protected synchronized boolean getTask() {
		int target = ((Integer) Config.getConfig().get(Config.TARGET)).intValue();
		if (target != lastTarget) {
			lastTarget = target;
			opsdone = 0;
			st = System.currentTimeMillis();
		}
		
		if (target > 0) {
			while ((System.currentTimeMillis() - st) / 1000 < (((double)opsdone) / target)) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
		if (!isAlive || ops <= 0) {
			return false;
		}
		ops--;
		opsdone++;
		return true;
	}

	public synchronized void close() {
		if (isAlive) {
			isAlive = false;
		}
	}
	
	public void join() {
		// wait for all threads to finish
		Thread[] threads = new Thread[activeCount()];
		int count = enumerate(threads);
		for (int i = 0; i < count; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException ex) {
			}
		}
	}

	/**
	 * A PooledThread is a Thread in a ThreadPool group, designed to run tasks
	 * (Runnables).
	 */
	private class PooledThread extends Thread {
		private Workload workload;
		private DataStore db;
		
		public PooledThread(Workload workload, DataStore db) {
			super(ClientThreadPool.this, "PooledThread-" + (threadID++));
			this.workload = workload;
			this.db = db;
		}

		public void run() {
			while (!isInterrupted() && getTask()) {
				workload.doOperation(db);
			}
			
			// TODO: Probably shouldn't be here
			try {
				db.cleanup();
			} catch (DataStoreException e) {
				e.printStackTrace();
			}
			
			System.out.println("Client Thread Done");
		}
	}
}
