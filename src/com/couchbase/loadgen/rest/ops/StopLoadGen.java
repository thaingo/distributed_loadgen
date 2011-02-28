package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.client.Client;

public class StopLoadGen extends ServerResource {

	@Get
	public String represent() {
		Client.getClient().stop();
		return "Load Generation Stopped\n";
	}

}
