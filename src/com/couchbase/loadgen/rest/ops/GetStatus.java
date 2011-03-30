package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.cluster.ClusterManager;

public class GetStatus extends ServerResource {

	@Get
	public String represent() {
		if (ClusterManager.getManager().getClusterStatus())
			return "Running\n";
		return "Not Running\n";
	}

}