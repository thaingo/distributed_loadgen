package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.Config;

public class GetConfig  extends ServerResource {

	@Get
	public String represent() {
		String json = Config.getConfig().getConfigJson();
		
		if (json == null)
			return "Server Error: Unable to generator JSON\n";
		return json + "\n";
	}

}
