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

package com.couchbase.loadgen.workloads;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.DataStore;
import com.couchbase.loadgen.Utils;
import com.couchbase.loadgen.generator.CounterGenerator;
import com.couchbase.loadgen.generator.DiscreteGenerator;
import com.couchbase.loadgen.generator.Generator;
import com.couchbase.loadgen.generator.IntegerGenerator;
import com.couchbase.loadgen.generator.UniformIntegerGenerator;
import com.couchbase.loadgen.memcached.Memcached;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD
 * operations. The relative proportion of different kinds of operations, and
 * other properties of the workload, are controlled by parameters specified at
 * runtime.
 * 
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one
 * (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all
 * fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads
 * (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates
 * (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts
 * (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans
 * (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be
 * read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select
 * the records to operate on - uniform, zipfian or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to
 * scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be
 * used to choose the number of records to scan, for each scan, between 1 and
 * maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key
 * ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class MemcachedCoreWorkload extends Workload {
	private IntegerGenerator keysequence;
	private DiscreteGenerator operationchooser;
	private IntegerGenerator keychooser;
	private Generator fieldchooser;
	private CounterGenerator transactioninsertkeysequence;

	private boolean orderedinserts;
	private String insert_order = "hashed";
	private boolean dotransactions;
	
	public int field_count = 10;
	public String key_prefix = "user";
	public int value_length = 2048;

	public MemcachedCoreWorkload(IntegerGenerator keychooser) {
		int recordcount = ((Integer)Config.getConfig().get(Config.RECORD_COUNT)).intValue();
		int insertstart = ((Integer)Config.getConfig().get(Config.INSERT_START)).intValue();
		dotransactions = ((Boolean)Config.getConfig().get(Config.DO_TRANSACTIONS)).booleanValue();
		this.keychooser = keychooser;
		
		if (insert_order.compareTo("hashed") == 0) {
			orderedinserts = false;
		} else {
			orderedinserts = true;
		}

		keysequence = new CounterGenerator(insertstart);
		operationchooser = new DiscreteGenerator();
		transactioninsertkeysequence = new CounterGenerator(recordcount);
		
		Config cfg = Config.getConfig();
		if (((Double)cfg.get(Config.MEMADD)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMADD)).doubleValue(), "ADD");
		} else if (((Double)cfg.get(Config.MEMAPPEND)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMAPPEND)).doubleValue(), "APPEND");
		} else if (((Double)cfg.get(Config.MEMCAS)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMCAS)).doubleValue(), "CAS");
		} else if (((Double)cfg.get(Config.MEMDECR)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMDECR)).doubleValue(), "DECR");
		} else if (((Double)cfg.get(Config.MEMDELETE)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMDELETE)).doubleValue(), "DELETE");
		} else if (((Double)cfg.get(Config.MEMGET)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMGET)).doubleValue(), "GET");
		} else if (((Double)cfg.get(Config.MEMGETS)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMGETS)).doubleValue(), "GETS");
		} else if (((Double)cfg.get(Config.MEMINCR)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMINCR)).doubleValue(), "INCR");
		} else if (((Double)cfg.get(Config.MEMPREPEND)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMPREPEND)).doubleValue(), "PREPEND");
		} else if (((Double)cfg.get(Config.MEMREPLACE)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMREPLACE)).doubleValue(), "REPLACE");
		} else if (((Double)cfg.get(Config.MEMSET)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMSET)).doubleValue(), "SET");
		} else if (((Double)cfg.get(Config.MEMUPDATE)).doubleValue() > 0) {
			operationchooser.addValue(((Double)cfg.get(Config.MEMUPDATE)).doubleValue(), "UPDATE");
		}

		fieldchooser = new UniformIntegerGenerator(0, field_count - 1);
	}
	
	public boolean doOperation(DataStore memcached) {
		if (dotransactions)
			return doTransaction(memcached);
		else
			return doInsert(memcached);
	}

	/**
	 * Do one insert operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	public boolean doInsert(DataStore memcached) {
		int keynum = keysequence.nextInt();
		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String dbkey = key_prefix + keynum;
		String value = Utils.ASCIIString(value_length);
		
		if (((Memcached)memcached).set(dbkey, value) == 0)
			return true;
		else
			return false;
	}

	/**
	 * Do one transaction operation. Because it will be called concurrently from
	 * multiple client threads, this function must be thread safe. However,
	 * avoid synchronized, or the threads will block waiting for each other, and
	 * it will be difficult to reach the target throughput. Ideally, this
	 * function would have no side effects other than DB operations.
	 */
	public boolean doTransaction(DataStore memcached) {
		String op = operationchooser.nextString();

		if (op.compareTo("ADD") == 0) {
			doTransactionAdd((Memcached)memcached);
		} else if (op.compareTo("APPEND") == 0) {
			doTransactionAppend((Memcached)memcached);
		} else if (op.compareTo("CAS") == 0) {
			doTransactionCas((Memcached)memcached);
		} else if (op.compareTo("DECR") == 0) {
			doTransactionDecr((Memcached)memcached);
		} else if (op.compareTo("DELETE") == 0) {
			doTransactionDelete((Memcached)memcached);
		} else if (op.compareTo("GET") == 0) {
			doTransactionGet((Memcached)memcached);
		} else if (op.compareTo("GETS") == 0) {
			doTransactionGets((Memcached)memcached);
		} else if (op.compareTo("INCR") == 0) {
			doTransactionIncr((Memcached)memcached);
		} else if (op.compareTo("PREPEND") == 0) {
			doTransactionPrepend((Memcached)memcached);
		} else if (op.compareTo("REPLACE") == 0) {
			doTransactionReplace((Memcached)memcached);
		} else if (op.compareTo("SET") == 0) {
			doInsert((Memcached)memcached);
		} else if (op.compareTo("UPDATE") == 0) {
			doTransactionUpdate((Memcached)memcached);
		}
		return true;
	}
	
	public void doTransactionAdd(Memcached memcached) {
		// choose the next key
		int keynum = transactioninsertkeysequence.nextInt();
		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String dbkey = key_prefix + keynum;
		String value = Utils.ASCIIString(value_length);
		memcached.add(dbkey, value);
	}
	
	public void doTransactionAppend(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = key_prefix + keynum;
		memcached.append(key, 0, "appended_string");
	}
	
	public void doTransactionCas(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = key_prefix + keynum;
		long cas = memcached.gets(key);
		String value = Utils.ASCIIString(value_length);
		memcached.cas(key, cas, value);
	}
	
	public void doTransactionDecr(Memcached memcached) {
		
	}
	
	public void doTransactionDelete(Memcached memcached) {
		
	}

	public void doTransactionGet(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = key_prefix + keynum;

		memcached.get(keyname, null);
	}
	
	public long doTransactionGets(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		return memcached.gets(key_prefix + keynum);
	}
	
	public void doTransactionIncr(Memcached memcached) {
		
	}
	
	public void doTransactionPrepend(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = key_prefix + keynum;
		memcached.prepend(key, 0, "prepended_string");
	}
	
	public void doTransactionReplace(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String key = key_prefix + keynum;
		String value = Utils.ASCIIString(value_length);
		memcached.replace(key, value);
	}
	
	public void doTransactionSet(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = key_prefix + keynum;
		String value = Utils.ASCIIString(value_length);
		memcached.set(keyname, value);
	}
	
	public void doTransactionUpdate(Memcached memcached) {
		int keynum;
		do {
			keynum = keychooser.nextInt();
		} while (keynum > transactioninsertkeysequence.lastInt());

		if (!orderedinserts) {
			keynum = Utils.hash(keynum);
		}
		String keyname = key_prefix + keynum;
		String value = Utils.ASCIIString(value_length);
		memcached.update(keyname, value);
	}
}
