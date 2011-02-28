package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.client.Client;
import com.couchbase.loadgen.cluster.ClusterManager;
import com.couchbase.loadgen.cluster.Message;
import com.sun.enterprise.ee.cms.core.GMSException;

public class RunLoadGen extends ServerResource {

	@Get
	public String represent() {
		try {
			Message message = new Message();
			message.setOpcode(Message.OP_EXECUTE);
			ClusterManager.getManager().sendMessage(message);
			Client.getClient().execute();
			return "Starting Load Generation\n";
		} catch (GMSException e) {
			return "Error starting Load Generator\n";
		}
		
	}

}
