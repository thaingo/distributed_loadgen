package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.cluster.ClusterManager;
import com.couchbase.loadgen.cluster.Message;
import com.sun.enterprise.ee.cms.core.GMSException;

public class RunLoadGen extends ServerResource {

	@Get
	public String represent() {
		System.out.println("Attempting start");

		try {
			Message message = new Message();
			message.setOpcode(Message.OP_START);
			ClusterManager.getManager().sendMessage(message);
		} catch (GMSException e) {
			return "Error starting Load Generator\n";
		}
		if (ClusterManager.getManager().startLoadGeneration())
			return "Starting Load Generation\n";
		return "Error starting Load Generator\n";
	}

}
