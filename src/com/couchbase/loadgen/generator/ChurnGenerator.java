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

package com.couchbase.loadgen.generator;

import java.util.Random;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.cluster.ClusterManager;

/**
 * Generate a popularity distribution of items, skewed to favor recent items
 * significantly more than older items.
 */
public class ChurnGenerator extends IntegerGenerator {
	int recordcount;
	Random _r;
	int[] workingset;
	int workingsetsize;
	int workingsetmaxsize;
	int ops;
	

	public ChurnGenerator() {
		_r = new Random();
		this.workingsetsize = 0;
		this.workingsetmaxsize = ((Integer)Config.getConfig().get(Config.WORKING_SET)) / ClusterManager.getManager().getClusterSize();
		this.workingset = new int[workingsetmaxsize];
		this.recordcount = ((Integer)Config.getConfig().get(Config.RECORD_COUNT));
		ops = 0;
		nextInt();
	}

	/**
	 * Generate the next string in the distribution, skewed Zipfian favoring the
	 * items most recently returned by the basis generator.
	 */
	public int nextInt() {
		int workingsetdelta = ((Integer)Config.getConfig().get(Config.CHURN_DELTA));
		
		while (workingsetsize < workingsetmaxsize) {
			workingset[workingsetsize] = _r.nextInt(recordcount);
			workingsetsize++;
		}
		if (ops > workingsetdelta) {
			ops = 0;
			int index = _r.nextInt(workingsetsize);
			int next_val = _r.nextInt(recordcount);
			workingset[index] = next_val;
		}
		ops++;
		return workingset[_r.nextInt(workingsetmaxsize)];
	}
	
	/**
	 * 
	 * @param args
	 */
	public void resizeWorkingSet() {
		
	}

	public static void main(String[] args) {
		int[] keys = new int[((Integer)Config.getConfig().get(Config.RECORD_COUNT))];
		ChurnGenerator gen = new ChurnGenerator();
		for (int i = 0; i < Integer.parseInt(args[0]); i++) {
			keys[Integer.parseInt(gen.nextString())]++;
		}
		for (int i = 0; i < keys.length; i++) {
			System.out.println(i + ": " + keys[i]);
		}
	}
}
