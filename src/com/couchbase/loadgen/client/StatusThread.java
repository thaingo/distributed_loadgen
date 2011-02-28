package com.couchbase.loadgen.client;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.couchbase.loadgen.Config;
import com.couchbase.loadgen.cluster.ClusterManager;
import com.couchbase.loadgen.cluster.Message;
import com.couchbase.loadgen.measurements.Measurements;
import com.couchbase.loadgen.measurements.Stats;
import com.couchbase.loadgen.measurements.StatsID;
import com.couchbase.loadgen.measurements.exporter.MeasurementsExporter;
import com.couchbase.loadgen.measurements.exporter.TextMeasurementsExporter;
import com.sun.enterprise.ee.cms.core.GMSException;

/**
 * A thread to periodically show the status of the experiment, to reassure you
 * that progress is being made.
 * 
 * @author cooperb
 * 
 */
public class StatusThread extends Thread {
	LoadThread lt;

	public StatusThread(LoadThread lt) {
		this.lt = lt;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run() {
		long st = System.currentTimeMillis();
		long en = System.currentTimeMillis();
		long printstatsinterval;
		boolean alldone;
		int seconds = 0;
		lt.start();

		do {
			printstatsinterval = ((Integer)Config.getConfig().get(Config.PRINT_STATS_INTERVAL)).intValue();
			alldone = true;
			
			if (lt.getState() != Thread.State.TERMINATED)
				alldone = false;
			
			if (seconds >= printstatsinterval) {	
				Stats interval = Measurements.getMeasurements().getStats(StatsID.INTERVAL_STATS);
				int throughput = ((Integer)Config.getConfig().get(Config.TARGET)).intValue();
				
				if (!alldone) {
					en = System.currentTimeMillis();
					System.out.println(((en - st) / 1000) + " sec; " +  throughput + " throughput; "
							+ interval.getSummary() + "\n");
				}
				
				seconds = 0;
			}
			
			Stats current = Measurements.getMeasurements().getStats(StatsID.CURRENT_STATS);
			Message message = new Message();
			message.setOpcode(Message.OP_STATS);
			message.setBody(current.encodeJson().getBytes());
			try {
				ClusterManager.getManager().sendMessage(message);
			} catch (GMSException e) {
			}

			try {
				sleep(1000);
			} catch (InterruptedException e) {}
			seconds++;
		} while (!alldone);
		
		try {
			exportMeasurements(en - st);
		} catch (IOException e) {
			System.err.println("Could not export measurements, error: "+ e.getMessage());
			e.printStackTrace();
			System.exit(-1);
		}

		try {
			lt.join();
		} catch (InterruptedException e) {
		}
	}
	
	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * 
	 * @throws IOException
	 *             Either failed to write to output stream or failed to close
	 *             it.
	 */
	private void exportMeasurements(long runtime) throws IOException {
		MeasurementsExporter exporter = null;
		try {
			// if no destination file is provided the results will be written to stdout
			OutputStream out;
			String exportFile = (String)Config.getConfig().get(Config.EXPORT_FILE);
			Stats stats = Measurements.getMeasurements().getStats(StatsID.TOAL_STATS);
			long opcount = stats.getOperations();
			if (exportFile.equals("")) {
				out = System.out;
			} else {
				out = new FileOutputStream(exportFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = (String)Config.getConfig().get(Config.EXPORTER);
			try {
				exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception e) {
				System.err.println("Could not find exporter " + exporterStr + ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);
			
			stats.exportMeasurements(exporter);
		} finally {
			if (exporter != null) {
				exporter.close();
			}
		}
	}
}