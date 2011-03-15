/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.couchbase.loadgen.measurements;

import java.io.IOException;
import java.util.HashMap;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

import com.couchbase.loadgen.JsonStringBuilder;
import com.couchbase.loadgen.measurements.exporter.MeasurementsExporter;

/**
 * Take measurements and maintain a histogram of a given metric, such as READ
 * LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementHistogram extends OneMeasurement {
	private static final long serialVersionUID = 8771477575164658300L;
	private Object lock = new Object();
	private static final int microhistogramlength = 5;
	private static final int millihistogramlength = 10;
	
	int[] microhistogram;
	int[] millihistogram;
	int histogramoverflow;
	int operations;
	long totallatency;

	int exp_offset;
	int min;
	int max;
	HashMap<Integer, int[]> returncodes;

	public OneMeasurementHistogram(String name) {
		super(name);
		microhistogram = new int[microhistogramlength];
		millihistogram = new int[millihistogramlength];
		histogramoverflow = 0;
		operations = 0;
		totallatency = 0;
		exp_offset = 8;
		min = -1;
		max = -1;
		returncodes = new HashMap<Integer, int[]>();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
	 */
	public synchronized void reportReturnCode(int code) {
		Integer Icode = code;
		if (!returncodes.containsKey(Icode)) {
			int[] val = new int[1];
			val[0] = 0;
			returncodes.put(Icode, val);
		}
		returncodes.get(Icode)[0]++;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.yahoo.ycsb.OneMeasurement#measure(int)
	 */
	public void measure(int latency) {
		synchronized(lock) {
			if (((int)Math.pow(2.0, (double)millihistogramlength) * 1000) < latency)
				histogramoverflow++;
			
			for (int i = 0; i < microhistogramlength; i++) {
				if (latency >= (i * 200) && latency < ((i + 1) * 200)) {
					microhistogram[i]++;
					break;
				}
			}
			
			for (int i = 0; i < millihistogramlength; i++) {
				int intervalmin = (int)Math.pow(2.0, (double)(i)) * 1000;
				int intervalmax = (int)Math.pow(2.0, (double)(i + 1)) * 1000;
				if (latency >= intervalmin && latency < intervalmax) {
					millihistogram[i]++;
					break;
				}
			}
			
			operations++;
			totallatency += latency;
			
			if ((min < 0) || (latency < min)) {
				min = latency;
			}
	
			if ((max < 0) || (latency > max)) {
				max = latency;
			}
		}
	}

	@Override
	public void exportMeasurements(MeasurementsExporter exporter) throws IOException {
		double mean = ((double)totallatency / (double)operations);
		exporter.write(getName(), "Operations", operations);
		exporter.write(getName(), "AverageLatency", computeTime(mean));
		exporter.write(getName(), "MinLatency", computeTime((double)min));
		exporter.write(getName(), "MaxLatency", computeTime((double)max));
		exporter.write(getName(), "95thPercentileLatency", getPercentile(.95));
		exporter.write(getName(), "99thPercentileLatency", getPercentile(.99));
		exporter.write(getName(), "99.9thPercentileLatency", getPercentile(.999));
		
		for (Integer I : returncodes.keySet()) {
			int[] val = returncodes.get(I);
			exporter.write(getName(), "Return=" + I, val[0]);
		}
		
		for (int i = 0; i < microhistogram.length; i++) {
			String key = computeTime(i * 200) + " - " + computeTime((i + 1) * 200);
			exporter.write(getName(), key, microhistogram[i]);
		}
		for (int i = 0; i < millihistogram.length; i++) {
			int intervalmin = (int)Math.pow(2.0, (double)(i)) * 1000;
			int intervalmax = (int)Math.pow(2.0, (double)(i + 1)) * 1000;
			String key = computeTime(intervalmin) + " - " + computeTime(intervalmax);
			exporter.write(getName(), key, millihistogram[i]);
		}

		String overflowtime = computeTime((int)Math.pow(2.0, (double)(millihistogramlength)) * 1000);
		exporter.write(getName(), ">" + overflowtime, histogramoverflow);
	}
	
	@Override
	public String getSummary() {
		if (operations == 0) {
			return "";
		}
		String avg = computeTime((int)(((double) totallatency) / ((double) operations)));
		String p99 = getPercentile(.99);
		
		return "[" + getName() + " total=" + operations + "  avg=" + avg + " 99th=" + p99 + "]";
	}
	
	public String getPercentile(double percentile) {
		int i;
		int opcounter = 0;
		for (i = 0; i < microhistogramlength; i++) {
			opcounter += microhistogram[i];
			if (((double) opcounter) / ((double) operations) >= percentile) {
				return computeTime((int)(i * 200));
			}
		}
		for (i = 0; i < millihistogramlength; i++) {
			opcounter += millihistogram[i];
			if (((double) opcounter) / ((double) operations) >= percentile) {
				return computeTime((int)(Math.pow(2, i) * 1000));
			}
		}
		return computeTime((int)(Math.pow(2, millihistogramlength) * 1000));
	}
			
	public void encodeJson(JsonStringBuilder builder) {
		try {
			builder.addElement("totallatency", new Double((double) totallatency));
			builder.openSubelement("stats");
			for (int i = 0; i < microhistogram.length; i++) {
				String key = computeTime(i * 200) + " - " + computeTime((i + 1) * 200);
				builder.addElement(key, new Integer(microhistogram[i]));
			}
			for (int i = 0; i < millihistogram.length; i++) {
				int intervalmin = (int)Math.pow(2.0, (double)(i)) * 1000;
				int intervalmax = (int)Math.pow(2.0, (double)(i + 1)) * 1000;
				String key = computeTime(intervalmin) + " - " + computeTime(intervalmax);
				builder.addElement(key, new Integer(millihistogram[i]));
			}
			builder.closeSubelement();
			
			builder.openSubelement("returncodes");
			for (Integer I : returncodes.keySet()) {
				int[] val = returncodes.get(I);
				builder.addElement(I.toString(), new Integer(val[0]));
			}
			builder.closeSubelement();
		} catch (JsonGenerationException e) {
		} catch (IOException e) {
		}
	}
	
	public void decodeJson(JsonParser p) {
		synchronized(lock) {
			try {
				p.nextToken();
				p.nextToken();
				if (p.getCurrentName().equals("totallatency")) {
					p.nextToken();
					totallatency += p.getLongValue();
					p.nextToken();
				}
				if (p.getCurrentName().equals("stats")) {
					p.nextToken();
					int statnum = 0;
					while(p.nextToken() != JsonToken.END_OBJECT) {
						JsonToken token = p.nextToken();
						if	(token == JsonToken.VALUE_NUMBER_INT) {
							if (statnum < microhistogramlength)
								microhistogram[statnum] += p.getIntValue();
							else
								millihistogram[statnum - microhistogramlength] += p.getIntValue();
							operations += p.getIntValue();
							statnum++;
						} else {
							//LOG.error("Field for " + key + " did not match type");
						}
					}
				}
				p.nextToken();
				if (p.getCurrentName().equals("returncodes")) {
					p.nextToken();
					while (p.nextToken() != JsonToken.END_OBJECT) {
						Integer code = new Integer(p.getCurrentName());
						int[] list = new int[1];
						JsonToken token = p.nextToken();
						list[0] = p.getIntValue();
						if	(token == JsonToken.VALUE_NUMBER_INT) {
							if (returncodes.containsKey(code))
								list[0] += returncodes.get(code)[0];
							returncodes.put(code, list);
						} else {
							//LOG.error("Field for " + key + " did not match type");
						}
					}
				}
				
			} catch (JsonParseException e) {
			} catch (IOException e) {
			}
		}
	}
}
