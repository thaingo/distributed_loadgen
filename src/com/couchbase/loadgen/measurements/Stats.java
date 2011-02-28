package com.couchbase.loadgen.measurements;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.JsonStringBuilder;
import com.couchbase.loadgen.measurements.exporter.MeasurementsExporter;

public class Stats {
	private HashMap<String, OneMeasurement> measurements;
	private long operations;
	
	public Stats() {
		this.measurements = new HashMap<String, OneMeasurement>();
		this.operations = 0;
	}
	
	public synchronized void measure(String operation, int latency) {
		if (!measurements.containsKey(operation)) {
			synchronized (this) {
				if (!measurements.containsKey(operation)) {
					measurements.put(operation, constructOneMeasurement(operation));
				}
			}
		}
		
		try {
			operations++;
			measurements.get(operation).measure(latency);
		} catch (java.lang.ArrayIndexOutOfBoundsException e) {
			System.out.println("ERROR: java.lang.ArrayIndexOutOfBoundsException - ignoring and continuing");
			e.printStackTrace();
			e.printStackTrace(System.out);
		}
	}
	
	OneMeasurement constructOneMeasurement(String name) {
		return new OneMeasurementHistogram(name);
	}
	
	public void reportReturnCode(String operation, int code) {
		if (!measurements.containsKey(operation)) {
			synchronized (this) {
				if (!measurements.containsKey(operation)) {
					measurements.put(operation, constructOneMeasurement(operation));
				}
			}
		}
		measurements.get(operation).reportReturnCode(code);
	}
	
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		for (OneMeasurement measurement : measurements.values()) {
			measurement.exportMeasurements(exporter);
		}
	}
	
	public long getOperations() {
		return operations;
	}
	
	public synchronized String getSummary() {
		int interval = ((Integer)Config.getConfig().get(Config.PRINT_STATS_INTERVAL)).intValue();
		
		String ret = " " + operations + " operations; " + (operations / interval) + " ops/sec";
		for (OneMeasurement m : measurements.values()) {
			ret += m.getSummary() + " ";
		}
		return ret;
	}
	
	public String encodeJson() {
		JsonStringBuilder builder = new JsonStringBuilder();
		
		try {
			builder.startJsonString();
			builder.addElement("ops", new Integer((int) operations));
			for (OneMeasurement m : measurements.values()) {
				builder.openSubelement(m.getName());
				m.encodeJson(builder);
				builder.closeSubelement();
			}
			builder.endJson();
		} catch (JsonGenerationException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		
		return builder.toString();
	}
	
	public void decodeJson(String json) {
		JsonFactory factory = new JsonFactory();
		try {
			JsonParser p = factory.createJsonParser(json.getBytes());
			p.nextToken();
			p.nextToken();
			if (p.getCurrentName().equals("ops")) {
				p.nextToken();
				operations += p.getIntValue();
			}
			while(p.nextToken() != JsonToken.END_OBJECT) {
				//System.out.println(p.getCurrentName());
				if (!measurements.containsKey(p.getCurrentName()))
					measurements.put(p.getCurrentName(), constructOneMeasurement(p.getCurrentName()));
				measurements.get(p.getCurrentName()).decodeJson(p);
			}
		} catch (JsonParseException e) {
		} catch (IOException e) {
		}
	}
}
