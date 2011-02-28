package com.couchbase.loadgen;

import java.io.IOException;
import java.io.StringWriter;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.impl.DefaultPrettyPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonStringBuilder {
	private static final Logger LOG = LoggerFactory.getLogger(Config.class);
	
	private StringWriter sw;
	private JsonFactory factory;
	private JsonGenerator g;
	private boolean isStarted;
	private boolean isEnded;
	
	public JsonStringBuilder() {
		sw = new StringWriter();
		factory = new JsonFactory();
		isStarted = false;
		isEnded = false;
	}
	
	public void startJsonString() {
		if (!isStarted) {
			isStarted = true;
			try {
				g = factory.createJsonGenerator(sw);
				g.setPrettyPrinter(new DefaultPrettyPrinter());
				g.writeStartObject();
			} catch (IOException e) {
				isStarted = false;
				LOG.error("Error creating JsonGenerator");
			}
		}
	}
	
	public void openSubelement(String fieldname) throws JsonGenerationException, IOException {
		g.writeFieldName(fieldname);
		g.writeStartObject();
	}
	
	public void addElement(String key, String value) throws JsonGenerationException, IOException {
		g.writeStringField(key, value);
	}
	
	public void addElement(String key, Boolean value) throws JsonGenerationException, IOException {
		g.writeBooleanField(key, value.booleanValue());
	}
	
	public void addElement(String key, Double value) throws JsonGenerationException, IOException {
		g.writeNumberField(key, value.doubleValue());
	}
	
	public void addElement(String key, Integer value) throws JsonGenerationException, IOException {
		g.writeNumberField(key, value.intValue());
	}
	
	public void closeSubelement() throws JsonGenerationException, IOException {
		g.writeEndObject();
	}
	
	public void endJson() throws JsonGenerationException, IOException {
		isEnded = true;
		g.writeEndObject();
		g.close();
	}
	
	@Override
	public String toString() {
		if (isEnded)
			return sw.toString();
		return null;
	}
}
