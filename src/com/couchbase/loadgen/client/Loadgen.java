package com.couchbase.loadgen.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.cluster.ClusterManager;
import com.couchbase.loadgen.exception.WorkloadException;
import com.couchbase.loadgen.generator.ChurnGenerator;
import com.couchbase.loadgen.generator.IntegerGenerator;
import com.couchbase.loadgen.generator.ScrambledZipfianGenerator;
import com.couchbase.loadgen.memcached.SpymemcachedClient;
import com.couchbase.loadgen.workloads.MemcachedCoreWorkload;
import com.couchbase.loadgen.workloads.Workload;

public class Loadgen extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(Loadgen.class);
	private Config cfg;
	StatsManager sm;
	ClientThreadPool pool;
	
	public Loadgen() {
		cfg = Config.getConfig();
	}
	
	public void run() {
		// Run the client threads
		int threadcount = ((Integer)Config.getConfig().get(Config.THREAD_COUNT)).intValue();
		IntegerGenerator keychooser = getRequestDistribution();
		Workload workload = new MemcachedCoreWorkload(keychooser);
		
		if (workload != null) {
			pool = new ClientThreadPool(threadcount, workload, SpymemcachedClient.CLASSNAME);
			pool.join();
			
			// Cleanup the worker threads workspace
			try {
				workload.cleanup();
			} catch (WorkloadException e) {
				e.printStackTrace(System.out);
				System.exit(0);
			}
		}
		
		ClusterManager.getManager().finishedLoadGeneration();
		LOG.info("Load generation finished");
	}
	
	private IntegerGenerator getRequestDistribution() {
		String rdistribution = (String)Config.getConfig().get(Config.REQUEST_DISTRIBUTION);
		
		if (rdistribution.equals("zipfian")) {
			int recordcount = ((Integer)cfg.get(Config.RECORD_COUNT)).intValue();
			int opcount = ((Integer)cfg.get(Config.OP_COUNT)).intValue();
			double memset = ((Double)cfg.get(Config.MEMSET)).doubleValue();
			int expectednewkeys = (int) (((double) opcount) * memset * 2.0);
			
			return new ScrambledZipfianGenerator(recordcount + expectednewkeys);
		} else if (rdistribution.equals("churn")) {
			return new ChurnGenerator();
		} else {
			LOG.error("Invalid request distribution");
			return null;
		}
	}
	
	public void terminate() {
		pool.close();
	}
}
