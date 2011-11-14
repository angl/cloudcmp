package org.cloudcmp.tasks.compute.memory.large;

import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;

public class MemoryLargeTask extends ComputeTask {
	public MemoryLargeTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}

	public String getTaskName() {
		return "memory.large";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}
	
	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		// in the large memory test, we allocate sequential memory > 2MB for many times
		// and each time we write/read each word of the memory		
		public final static int MEM_LENGTH = 1250000;
		public final static int ROUND = 100;
		
		public void realRun() {
			for (int i = 0; i < ROUND; ++ i) {
				long[] buf = new long [MEM_LENGTH];
				for (int j = 0; j < MEM_LENGTH; ++ j) {
					buf[j] = j;
				}
				long k = 0;
				for (int j = 0; j < MEM_LENGTH; ++ j) {
					k += buf[j];
				}
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
		MemoryLargeTask task = new MemoryLargeTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
