package com.couchbase.loadgen.cluster;

import java.util.List;

import org.restlet.Component;
import org.restlet.data.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.client.Loadgen;
import com.couchbase.loadgen.client.StatsManager;
import com.couchbase.loadgen.measurements.Measurements;
import com.couchbase.loadgen.rest.ClusterRest;
import com.sun.enterprise.ee.cms.core.CallBack;
import com.sun.enterprise.ee.cms.core.FailureNotificationSignal;
import com.sun.enterprise.ee.cms.core.FailureSuspectedSignal;
import com.sun.enterprise.ee.cms.core.GMSException;
import com.sun.enterprise.ee.cms.core.GMSFactory;
import com.sun.enterprise.ee.cms.core.GMSMember;
import com.sun.enterprise.ee.cms.core.GroupHandle;
import com.sun.enterprise.ee.cms.core.GroupManagementService;
import com.sun.enterprise.ee.cms.core.JoinNotificationSignal;
import com.sun.enterprise.ee.cms.core.MessageSignal;
import com.sun.enterprise.ee.cms.core.PlannedShutdownSignal;
import com.sun.enterprise.ee.cms.core.Signal;
import com.sun.enterprise.ee.cms.core.SignalAcquireException;
import com.sun.enterprise.ee.cms.core.SignalReleaseException;
import com.sun.enterprise.ee.cms.impl.client.FailureNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.FailureSuspectedActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.JoinNotificationActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.MessageActionFactoryImpl;
import com.sun.enterprise.ee.cms.impl.client.PlannedShutdownActionFactoryImpl;

public class ClusterManager implements CallBack {
	private static final Logger LOG = LoggerFactory.getLogger(ClusterManager.class);
	public static final String GROUP_NAME = "loadgenerator";
	private static ClusterManager cm = null;
	private GroupManagementService gms;
	private Loadgen lg;
	private StatsManager sm;
	private int nodesrunning;
	private boolean running;
	final Object waitLock = new Object();

	private ClusterManager() {
		nodesrunning = 0;
		running = false;
		
		initNode();
		joinNodeToCluster();
	}
	
	private void initNode() {
		String server = "server" + System.currentTimeMillis();
		gms = (GroupManagementService) GMSFactory.startGMSModule(server, GROUP_NAME, GroupManagementService.MemberType.CORE, null);
	}
	
	private void joinNodeToCluster() {
		try {
			gms.join();
			registerForGroupEvents(gms);
		} catch (GMSException e) {
			e.printStackTrace();
		}
	}
	
	public static ClusterManager getManager() {
		if (cm == null)
			cm = new ClusterManager();
		return cm;
	}
	
	private void registerForGroupEvents(GroupManagementService gms) {
        gms.addActionFactory(new JoinNotificationActionFactoryImpl(this));
        gms.addActionFactory(new FailureSuspectedActionFactoryImpl(this));
        gms.addActionFactory(new FailureNotificationActionFactoryImpl(this));
        gms.addActionFactory(new PlannedShutdownActionFactoryImpl(this));
        gms.addActionFactory(new MessageActionFactoryImpl(this), GROUP_NAME);
    }
	
	public void processNotification(Signal signal) {
		try {
			signal.acquire();
			if (signal instanceof MessageSignal) {
				Message message = Message.decode(((MessageSignal) signal).getMessage());
				if (message.getOpcode() == Message.OP_START) {
					startLoadGeneration();
				} else if (message.getOpcode() == Message.OP_FINISH) {
					nodesrunning--;
				} else if (message.getOpcode() == Message.OP_STOP) {
					stopLoadGeneration();
				} else if (message.getOpcode() == Message.OP_CONFIG) {
					Config.getConfig().setConfig(new String(message.getBody()));
				} else if (message.getOpcode() == Message.OP_STATS) {
					try {
					Measurements.getMeasurements().addMeasurement(new String(message.getBody()));
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println("Body: " + new String(message.getBody()));
					}
				}
			} else {
				if (signal instanceof JoinNotificationSignal) {
					System.out.println("Joining node " + signal.getMemberToken());
				} else if (signal instanceof FailureSuspectedSignal) {
					System.out.println("Failure Suspected " + signal.getMemberToken());
				} else if (signal instanceof FailureNotificationSignal) {
					System.out.println("Failure notification " + signal.getMemberToken());
				} else if (signal instanceof PlannedShutdownSignal) {
					System.out.println("Planned shutdown " + signal.getMemberToken());
				} else {
					System.out.println("Recieved unknown signal " + signal.toString());
				}
			}
			signal.release();
		} catch (SignalAcquireException e) {
			e.printStackTrace();
		} catch (SignalReleaseException e) {
			e.printStackTrace();
		}
	}
	
	public String getServerList() {
		GroupHandle gh = gms.getGroupHandle();
		String members = "";
		
		List<GMSMember> m = gh.getCurrentView();
		for (int i = 0; i < m.size(); i++) {
			members = members + m.get(i).getMemberToken() + "\n";
		}
		return members;
	}
	
	public int getClusterSize() {
		return gms.getGroupHandle().getCurrentView().size();
	}
	
	public void sendMessage(Message message) throws GMSException {
		GroupHandle gh = gms.getGroupHandle();
		gh.sendMessage(GROUP_NAME, message.encode());
	}
	
	public boolean getClusterStatus() {
		if (nodesrunning == 0)
			return false;
		return true;
	}
	
	public boolean startLoadGeneration() {
		if (nodesrunning == 0) {
			nodesrunning = gms.getGroupHandle().getCurrentView().size();
			running = true;
			lg = new Loadgen();
			sm = new StatsManager();
			lg.start();
			sm.start();
			return true;
		}
		LOG.error("Couldn't start load generation: Already running");
		return false;
	}
	
	public void finishedLoadGeneration() {
		if (running) {
			try {
				Message message = new Message();
				message.setOpcode(Message.OP_FINISH);
				sendMessage(message);
				sm.done();
				nodesrunning--;
				running = false;
			} catch (GMSException e) {
				LOG.error("Couldn't send finish message");
			}
		}
	}
	
	public void stopLoadGeneration() {
		if (running) {
			lg.terminate();
			finishedLoadGeneration();
		}
	}
	
	public static void main(String args[]) {
		ClusterManager.getManager();
		
	    try {
	    	Component component = new Component();
	    	component.getServers().add(Protocol.HTTP, 8182);  
		    component.getDefaultHost().attach("/cluster", new ClusterRest()); 
			component.start();
		} catch (Exception e) {
		} 
	}
}
