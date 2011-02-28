package com.couchbase.loadgen.rest.ops;

import org.restlet.resource.Get;
import org.restlet.resource.ServerResource;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.cluster.ClusterManager;
import com.couchbase.loadgen.cluster.Message;
import com.sun.enterprise.ee.cms.core.GMSException;

public class SetValue extends ServerResource {

	@Get
	public String represent() {
		if (getQuery().size() != 2)
			return "Incorrect parameter specification\n";
		String name = getQuery().get(0).getValue();
		String value = getQuery().get(1).getValue();
		if (Config.getConfig().set(name, value)) {
			Message message = new Message();
			message.setOpcode(Message.OP_CONFIG);
			message.setBody(Config.getConfig().getConfigJson().getBytes());
			try {
				ClusterManager.getManager().sendMessage(message);
			} catch (GMSException e) {
				return "Error sending message to others in the cluster\n";
			}
			return "Value Added\n";
		}
		return "Property doesn't exist\n";
	}

}
