package com.couchbase.loadgen.measurements;

public class Measurements {
	private static final long serialVersionUID = -311232927139188477L;
	private static Measurements measurements = null;
	private Stats total;
	private Stats interval;
	private Stats current;

	/**
	 * Return the singleton Measurements object.
	 */
	public synchronized static Measurements getMeasurements() {
		if (measurements == null)
			measurements = new Measurements();
		return measurements;
	}

	/**
	 * Create a new object with the specified properties.
	 */
	public Measurements() {
		total = new Stats();
		interval = new Stats();
		current = new Stats();
	}

	/**
	 * Report a single value of a single metric. E.g. for read latency,
	 * operation="READ" and latency is the measured value.
	 */
	public synchronized void measure(String operation, int latency) {
		total.measure(operation, latency);
		interval.measure(operation, latency);
		current.measure(operation, latency);
	}

	/**
	 * Report a return code for a single DB operaiton.
	 */
	public void reportReturnCode(String operation, int code) {
		total.reportReturnCode(operation, code);
		interval.reportReturnCode(operation, code);
		//current.reportReturnCode(operation, code);
	}

	public synchronized void addMeasurement(String json) {
		total.decodeJson(json);
		interval.decodeJson(json);
	}
	
	public synchronized Stats getStats(StatsID id) {
		Stats stats = null;
		if (id == StatsID.CURRENT_STATS) {
			stats = current;
			current = new Stats();
		} else if (id == StatsID.INTERVAL_STATS) {
			stats = interval;
			interval = new Stats();
		} else if (id == StatsID.TOAL_STATS) {
			stats = total;
			total = new Stats();
		}
		return stats;
	}
}
