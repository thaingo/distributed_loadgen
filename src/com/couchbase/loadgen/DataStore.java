package com.couchbase.loadgen;

import com.couchbase.loadgen.exception.DataStoreException;

public abstract class DataStore {

	/**
	 * Initialize any state for this DB. Called once per DB instance; there is
	 * one DB instance per client thread.
	 */
	public void init() throws DataStoreException {
	}

	/**
	 * Cleanup any state for this DB. Called once per DB instance; there is one
	 * DB instance per client thread.
	 */
	public void cleanup() throws DataStoreException {
	}
}
