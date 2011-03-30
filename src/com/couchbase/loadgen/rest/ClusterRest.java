package com.couchbase.loadgen.rest;

import org.restlet.Application;
import org.restlet.Restlet;
import org.restlet.routing.Router;

import com.couchbase.loadgen.rest.ops.GetStats;
import com.couchbase.loadgen.rest.ops.GetStatus;
import com.couchbase.loadgen.rest.ops.GetValue;
import com.couchbase.loadgen.rest.ops.SetValue;
import com.couchbase.loadgen.rest.ops.GetConfig;
import com.couchbase.loadgen.rest.ops.RunLoadGen;
import com.couchbase.loadgen.rest.ops.ServerList;
import com.couchbase.loadgen.rest.ops.StopLoadGen;

public class ClusterRest extends Application {

	/**
	 * Creates a root Restlet that will receive all incoming calls.
	 */
	@Override
	public synchronized Restlet createInboundRoot() {
		// Create a router Restlet that routes each call to a
		Router router = new Router(getContext());

		// Defines only one route
		router.attach("/server-list", ServerList.class);
		router.attach("/run", RunLoadGen.class);
		router.attach("/stop", StopLoadGen.class);
		router.attach("/get-config", GetConfig.class);
		router.attach("/get-value", GetValue.class);
		router.attach("/set-value", SetValue.class);
		router.attach("/get-stats", GetStats.class);
		router.attach("/get-status", GetStatus.class);

		return router;
	}

}
