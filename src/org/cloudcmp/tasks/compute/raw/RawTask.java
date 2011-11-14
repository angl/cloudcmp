package org.cloudcmp.tasks.compute.raw;

import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;

// raw cpu spinning task
public class RawTask extends ComputeTask {
	public RawTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}

	public String getTaskName() {
		return "raw";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}

	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		private static final int count = 100000000;
		
		public void realRun() {
			long sum = 0;
			for (long i = 0; i < count; ++ i) {
				sum ++;
			}
		}
	}
	
	public static void main(String [] args) {
		if (args.length < 4) {
			System.err.println("Arguments: [adaptor_name] [config_file] [num_threads] [num_runs]");
			System.exit(1);
		}
		Adaptor adaptor = Adaptor.getAdaptorByName(args[0]);
		if (adaptor == null) {
			System.err.println("Unknown adaptor");
			System.exit(1);
		}
		
		adaptor.loadConfigFromFile(args[1]);		
		RawTask task = new RawTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
