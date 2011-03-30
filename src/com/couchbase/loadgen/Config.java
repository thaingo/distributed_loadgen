package com.couchbase.loadgen;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Iterator;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Config {
	private static Config config = null;
	private static final Logger LOG = LoggerFactory.getLogger(Config.class);
	private static final String PROPERTIES_JSON_ID = "properties";
	public static final String CHURN_DELTA = "churndelta";
	public static final String DB = "db";
	public static final String DO_TRANSACTIONS = "dotransactions";
	public static final String EXPORTER = "exporter";
	public static final String EXPORT_FILE = "exportfile";
	public static final String INSERT_COUNT = "insertcount";
	public static final String INSERT_ORDER = "insertorder";
	public static final String INSERT_START = "insertstart";
	public static final String KEY_PREFIX = "keyprefix";
	public static final String LABEL = "label";
	public static final String MEASUREMENT_TYPE = "measurementtype";
	public static final String MEMCACHED_ADDRESS = "memcached.address";
	public static final String MEMCACHED_PORT = "memcached.port";
	public static final String MEMADD = "memaddproportion";
	public static final String MEMAPPEND = "memappendproportion";
	public static final String MEMCAS = "memcasproportion";
	public static final String MEMDECR = "memdecrproportion";
	public static final String MEMDELETE = "memdeleteproportion";
	public static final String MEMGET = "memgetproportion";
	public static final String MEMGETS = "memgetsproportion";
	public static final String MEMINCR = "memincrproporiton";
	public static final String MEMPREPEND = "memprependproportion";
	public static final String MEMREPLACE = "memreplaceproportion";
	public static final String MEMSET = "memsetproportion";
	public static final String MEMUPDATE = "memupdateproportion";
	public static final String OP_COUNT = "operationcount";
	public static final String PROTOCOL = "protocol";
	public static final String PRINT_STATS_INTERVAL = "printstatsinterval";
	public static final String RECORD_COUNT = "recordcount";
	public static final String REQUEST_DISTRIBUTION = "requestdistribution";
	public static final String TARGET = "target";
	public static final String THREAD_COUNT = "threadcount";
	public static final String VALUE_LENGTH = "valuelength";
	public static final String WORKING_SET = "workingset";
	public static final String WORKLOAD = "workload";

	private HashMap<String, Object> properties;

	private Config() {
		properties = new HashMap<String, Object>();
		properties.put(CHURN_DELTA, new Integer(1));
		properties.put(DB, new String());
		properties.put(DO_TRANSACTIONS, new Boolean(true));
		properties.put(EXPORTER, "com.couchbase.loadgen.measurements.exporter.TextMeasurementsExporter");
		properties.put(EXPORT_FILE, new String());
		properties.put(INSERT_ORDER, "hashed");
		properties.put(INSERT_START, new Integer(0));
		properties.put(KEY_PREFIX, "user");
		properties.put(LABEL, "");
		properties.put(MEASUREMENT_TYPE, "histogram");
		properties.put(MEMCACHED_ADDRESS, "10.2.1.15");
		properties.put(MEMCACHED_PORT, new Integer(11211));
		properties.put(MEMADD, new Double(0.0));
		properties.put(MEMAPPEND, new Double(0.0));
		properties.put(MEMCAS, new Double(0.0));
		properties.put(MEMDECR, new Double(0.0));
		properties.put(MEMDELETE, new Double(0.0));
		properties.put(MEMGET, new Double(1.0));
		properties.put(MEMGETS, new Double(0.0));
		properties.put(MEMINCR, new Double(0.0));
		properties.put(MEMPREPEND, new Double(0.0));
		properties.put(MEMREPLACE, new Double(0.0));
		properties.put(MEMSET, new Double(0.0));
		properties.put(MEMUPDATE, new Double(0.0));
		properties.put(OP_COUNT, new Integer(10000));
		properties.put(PRINT_STATS_INTERVAL, new Integer(5));
		properties.put(PROTOCOL, "ascii");
		properties.put(RECORD_COUNT, new Integer(10000));
		properties.put(REQUEST_DISTRIBUTION, "zipfian");
		properties.put(TARGET, new Integer(5000));
		properties.put(THREAD_COUNT, new Integer(1));
		properties.put(WORKING_SET, new Integer(5));
		properties.put(VALUE_LENGTH, new Integer(256));
	}
	
	public static Config getConfig() {
		if (config == null)
			return (config = new Config());
		return config;
	}
	
	public void setConfig(String configjson) {
		JsonFactory factory = new JsonFactory();
		
		try {
			JsonParser p = factory.createJsonParser(configjson.getBytes());
			p.nextToken();
			p.nextToken();
			if (p.getCurrentName().equals(PROPERTIES_JSON_ID)) {
				p.nextToken();
				while(p.nextToken() != JsonToken.END_OBJECT) {
					
					String key = p.getCurrentName();
					JsonToken token = p.nextToken();
					if	(token == JsonToken.VALUE_NUMBER_INT && properties.get(key) instanceof Integer) {
						properties.put(key, new Integer(p.getIntValue()));
					} else if (token == JsonToken.VALUE_NUMBER_FLOAT && properties.get(key) instanceof Double) {
						properties.put(key, new Double(p.getDoubleValue()));
					} else if ((token == JsonToken.VALUE_FALSE || token == JsonToken.VALUE_TRUE) && properties.get(key) instanceof Boolean) {
						properties.put(key, new Boolean(p.getBooleanValue()));
					} else if (token == JsonToken.VALUE_STRING && properties.get(key) instanceof String){
						properties.put(key, p.getText());
					} else {
						LOG.error("Field for " + key + " did not match type");
					}
				}
			} else {
				LOG.error("Attempted to set an invalid config");
			}
		} catch (JsonParseException e) {
			LOG.error("Attempted to set an invalid config");
		} catch (IOException e) {
			LOG.error("Attempted to set an invalid config");
		}
	}
	
	public boolean set(String key, String value) {
		if (!properties.containsKey(key))
			return false;
		
		if (properties.get(key) instanceof String)
			properties.put(key, value);
		else if (properties.get(key) instanceof Boolean)
			properties.put(key, new Boolean(value));
		else if (properties.get(key) instanceof Integer)
			properties.put(key, new Integer(value));
		else if (properties.get(key) instanceof Double)
			properties.put(key, new Double(value));
		
		return true;
	}
	
	public Object get(String key) {
		return properties.get(key);
	}
	
	public String getPropertyJson(String key) {
		if (properties.containsKey(key)) {
			JsonStringBuilder builder = new JsonStringBuilder();
			builder.startJsonString();
			
			try {
				Object value = properties.get(key);
				System.out.println(key + " " + value);
				if (value instanceof String)
					builder.addElement(key, (String) value);
				else if (value instanceof Boolean)
					builder.addElement(key, ((Boolean) value));
				else if (value instanceof Integer)
					builder.addElement(key, ((Integer) value));
				else if (value instanceof Double)
					builder.addElement(key, ((Double) value));
				builder.endJson();
			} catch (JsonGenerationException e) {
				return null;
			} catch (IOException e) {
				return null;
			}
			System.out.println(builder.toString());
			return builder.toString();
		}
		return null;
	}
	
	public String getConfigJson() {
		StringWriter sw = new StringWriter();
		JsonFactory factory = new JsonFactory();
		JsonGenerator g;
		
		try {
			g = factory.createJsonGenerator(sw);
			g.setPrettyPrinter(new DefaultPrettyPrinter());
			g.writeStartObject();
			g.writeFieldName(PROPERTIES_JSON_ID);
			g.writeStartObject();
			
			// TODO: Can I sort this
			Iterator<String> itr = properties.keySet().iterator();
			String key;
			Object value;
			
			while (itr.hasNext()) {
				key = itr.next();
				value = properties.get(key);
				
				if (value instanceof String)
					g.writeStringField(key, (String) value);
				else if (value instanceof Boolean)
					g.writeBooleanField(key, ((Boolean) value).booleanValue());
				else if (value instanceof Integer)
					g.writeNumberField(key, ((Integer) value).intValue());
				else if (value instanceof Double)
					g.writeNumberField(key, ((Double) value).doubleValue());
			}
			
			g.writeEndObject();
			g.writeEndObject();
			g.close();
		} catch (JsonGenerationException e) {
			return null;
		} catch (IOException e) {
			return null;
		}
		return sw.toString();
	}
}
