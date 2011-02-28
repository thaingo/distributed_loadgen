package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.cluster.ClusterManager;

public class ServerList extends ServerResource {

	@Get
	public String represent() {
		return ClusterManager.getManager().getServerList();
	}

}
