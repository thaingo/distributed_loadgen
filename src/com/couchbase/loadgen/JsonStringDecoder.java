package com.couchbase.loadgen;

import java.io.IOException;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;

public class JsonStringDecoder {
	JsonFactory factory;
	JsonParser p;
	
	public JsonStringDecoder(String json) throws JsonParseException, IOException {
		factory = new JsonFactory();
		p = factory.createJsonParser(json.getBytes());
		p.nextToken();
	}
	
	/*public void setConfig(String configjson) {
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
	}*/
}
