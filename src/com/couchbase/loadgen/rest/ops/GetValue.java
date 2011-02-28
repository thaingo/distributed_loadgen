package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.Config;

public class GetValue extends ServerResource {

	@Get
	public String represent() {
		if (getQuery().size() != 1)
			return "Incorrect parameter specification\n";
		String name = getQuery().get(0).getValue();
		String json = Config.getConfig().getPropertyJson(name);
		if (json == null)
			return "Property " + name + " not found\nor value type is incorrect\n";
		return json + "\n";
	}

}
