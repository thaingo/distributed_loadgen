package com.couchbase.loadgen.measurements;

public enum StatsID {
	TOAL_STATS((byte) 0x0a),
	INTERVAL_STATS((byte) 0x20),
	CURRENT_STATS((byte) 0x21);

	public byte id;

	StatsID(byte id) {
		this.id = id;
	}
}
