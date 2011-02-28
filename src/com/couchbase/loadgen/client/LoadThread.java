/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.couchbase.loadgen.client;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.exception.WorkloadException;
import com.couchbase.loadgen.memcached.SpymemcachedClient;
import com.couchbase.loadgen.workloads.Workload;


/**
 * Main class for executing YCSB.
 */
public class LoadThread extends Thread {
	private Workload workload;
	private int threadcount;
	private int opcount;
	ClientThreadPool pool;
	
	public LoadThread(Workload workload) {
		this.threadcount = ((Integer)Config.getConfig().get(Config.THREAD_COUNT)).intValue();
		this.opcount = ((Integer)Config.getConfig().get(Config.OP_COUNT)).intValue();
		this.workload = workload;
		
		if (((Boolean)Config.getConfig().get(Config.DO_TRANSACTIONS)).booleanValue())
			opcount = ((Integer)Config.getConfig().get(Config.RECORD_COUNT)).intValue();
	}
	
	public void run() {
		// Run the client threads
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
	
	public void terminate() {
		pool.close();
	}
}
