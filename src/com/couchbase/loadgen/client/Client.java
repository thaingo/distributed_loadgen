package com.couchbase.loadgen.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.client.LoadThread;
import com.couchbase.loadgen.generator.IntegerGenerator;
import com.couchbase.loadgen.generator.ScrambledZipfianGenerator;
import com.couchbase.loadgen.workloads.MemcachedCoreWorkload;
import com.couchbase.loadgen.workloads.Workload;

public class Client {
	private static final Logger LOG = LoggerFactory.getLogger(Client.class);
	private static Client client = null;
	private Config cfg;
	private boolean isRunning;
	LoadThread lt;
	StatusThread st;
	Workload _workload;
	
	
	private Client() {
		this.isRunning = false;
		this.lt = null;
		this.st = null;
		cfg = Config.getConfig();
		
		int opcount = ((Integer)cfg.get(Config.OP_COUNT)).intValue();
		double memset = ((Double)cfg.get(Config.MEMSET)).doubleValue();
		int record_count = ((Integer)cfg.get(Config.RECORD_COUNT)).intValue();
		int expectednewkeys = (int) (((double) opcount) * memset * 2.0);
		IntegerGenerator keychooser = new ScrambledZipfianGenerator(record_count + expectednewkeys);
		this._workload = new MemcachedCoreWorkload(keychooser);
	}
	
	public static Client getClient() {
		if (client == null)
			client = new Client();
		return client;
	}
	
	public void execute() {
		LOG.info("Starting load generation");
		
		if (!isRunning) {
			lt = new LoadThread(_workload);
			st = new StatusThread(lt);
			st.start();
			isRunning = true;
		}
	}
	
	public void stop() {
		if (isRunning) {
			lt.terminate();
			try {
				lt.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			isRunning = false;
		}
	}
}
