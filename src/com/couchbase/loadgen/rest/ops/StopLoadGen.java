package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.cluster.ClusterManager;
import com.couchbase.loadgen.cluster.Message;
import com.sun.enterprise.ee.cms.core.GMSException;

public class StopLoadGen extends ServerResource {

	@Get
	public String represent() {
		try {
			Message message = new Message();
			message.setOpcode(Message.OP_STOP);
			ClusterManager.getManager().sendMessage(message);
			ClusterManager.getManager().stopLoadGeneration();
		} catch (GMSException e) {
			return "Error stopping loadgen";
		}
		ClusterManager.getManager().stopLoadGeneration();
		return "Load Generation Stopped\n";
	}

}
