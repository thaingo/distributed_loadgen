package com.couchbase.loadgen.memcached;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.cluster.ClusterManager;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.CASResponse;
import net.spy.memcached.MemcachedClient;

public class SpymemcachedClient extends Memcached {
	private static final Logger LOG = LoggerFactory.getLogger(SpymemcachedClient.class);
	public static final String CLASSNAME = "com.couchbase.loadgen.memcached.SpymemcachedClient";
	public static final String PROTO_ASCII = "ascii";
	public static final String PROTO_BINARY = "binary";
	MemcachedClient client;
	
	public static long endtime;
	Random random;
	boolean verbose;
	int todelay;

	public SpymemcachedClient() {
		random = new Random();
		todelay = 0;
	}
	
	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() {
		int membaseport = ((Integer) Config.getConfig().get(Config.MEMCACHED_PORT)).intValue();
		String addr = (String) Config.getConfig().get(Config.MEMCACHED_ADDRESS);
		String protocol = (String) Config.getConfig().get(Config.PROTOCOL);
		try {
			InetSocketAddress ia = new InetSocketAddress(InetAddress.getByAddress(ipv4AddressToByte(addr)), membaseport);
			if (protocol.equals(PROTO_BINARY)) {
				client = new MemcachedClient(new BinaryConnectionFactory(), Arrays.asList(ia));
			} else if (protocol.equals(PROTO_ASCII)) {
				client = new MemcachedClient(ia);
			} else {
				LOG.info("ERROR: BAD PROTOCOL");
				ClusterManager.getManager().finishedLoadGeneration();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	public void cleanup() {
		if (client.isAlive())
			client.shutdown();
	}
	
	@Override
	public int add(String key, Object value) {
		try {
			if (!client.add(key, 0, value).get().booleanValue()) {
				System.out.println("ADD: error getting data");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("ADD Interrupted");
		} catch (ExecutionException e) {
			System.out.println("ADD Execution");
		} catch (RuntimeException e) {
			System.out.println("ADD Runtime");
		}
		return 0;
	}
	
	@Override
	public int get(String key, Object value) {
		Future<Object> f = client.asyncGet(key);
		try {
			if (f.get() == null) {
				System.out.println("GET: error getting data");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("GET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("GET Execution");
			e.printStackTrace();
			return -2;
		} catch (RuntimeException e) {
			System.out.println("GET Runtime");
			return -3;
		}
		return 0;
	}
	
	/*public long qGet(String key, Object value) {
		return client.ag(key);
	}*/
	
	@Override
	public int set(String key, Object value) {
		try {
			if (!client.set(key, 0, value).get().booleanValue()) {
				System.out.println("SET: error getting data");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("SET Interrupted");
		} catch (ExecutionException e) {
			System.out.println("SET Execution");
		} catch (RuntimeException e) {
			System.out.println("SET Runtime");
		}
		return 0;
	}
	
	private byte[] ipv4AddressToByte(String address) {
		byte[] b = new byte[4];
		String[] str = address.split("\\.");
		b[0] = Integer.valueOf(str[0]).byteValue();
		b[1] = Integer.valueOf(str[1]).byteValue();
		b[2] = Integer.valueOf(str[2]).byteValue();
		b[3] = Integer.valueOf(str[3]).byteValue();
		return b;
	}

	@Override
	public int append(String key, long cas, Object value) {
		try {
			if (!client.append(cas, key, value).get().booleanValue())
				System.out.println("APPEND: error getting data");
				return -1;
		} catch (InterruptedException e) {
			System.out.println("APPEND Interrupted");
		} catch (ExecutionException e) {
			System.out.println("APPEND Execution");
		} catch (RuntimeException e) {
			System.out.println("APPEND Runtime");
		}
		return 0;
	}

	@Override
	public int cas(String key, long cas, Object value) {
		if (!client.cas(key, cas, value).equals(CASResponse.OK)) {
			System.out.println("CAS: error getting data");
			return -1;
		}
		return 0;
	}

	@Override
	public int decr(String key, Object value) {
		return 0;
	}

	@Override
	public int delete(String key) {
		return 0;
	}

	@Override
	public int incr(String key, Object value) {
		return 0;
	}

	@Override
	public long gets(String key) {
		long cas = client.gets(key).getCas();
		if (cas < 0) {
			System.out.println("GETS: error getting data");
			return -1;
		}
		return cas;
	}

	@Override
	public int prepend(String key, long cas, Object value) {
		try {
			if (!client.prepend(cas, key, value).get().booleanValue())
				return -1;
		} catch (InterruptedException e) {
			System.out.println("PREPEND Interrupted");
		} catch (ExecutionException e) {
			System.out.println("PREPEND Execution");
		} catch (RuntimeException e) {
			System.out.println("PREPEND Runtime");
		}
		return 0;
	}

	@Override
	public int replace(String key, Object value) {
		try {
			if (!client.replace(key, 0, value).get().booleanValue()) {
				System.out.println("REPLACE: error getting data");
				return -1;
			}
		} catch (InterruptedException e) {
			System.out.println("REPLACE Interrupted");
		} catch (ExecutionException e) {
			System.out.println("REPLACE Execution");
		} catch (RuntimeException e) {
			System.out.println("REPLACE Runtime");
		}
		return 0;
	}

	@Override
	public int update(String key, Object value) {
		return set(key, value);
	}
}
