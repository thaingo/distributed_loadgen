package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.measurements.Measurements;
import com.couchbase.loadgen.measurements.Stats;
import com.couchbase.loadgen.measurements.StatsID;

public class GetStats extends ServerResource {

	@Get
	public String represent() {
		Stats current = Measurements.getMeasurements().getStats(StatsID.INTERVAL_STATS);
		if (current == null)
			return "Couldn't get stats";
		return current.encodeJson() + "\n";
	}

}