package org.cloudcmp.tasks.compute.memory.xlarge;
import java.util.Map;
import java.util.Random;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;

public class MemoryXLargeTask extends ComputeTask {
	public MemoryXLargeTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}
	
	public String getTaskName() {
		return "memory.xlarge";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}
	
	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		// we allocate a 128MB memory and perform random read/write
		
		public final static int MEM_LENGTH = 100000000 / 4;
		public final static int ROUND = 20;
		
		public void realRun() {
			for (int r = 0; r < ROUND; ++ r) {
				int[] buf = new int[MEM_LENGTH];
				for (int i = 0; i < 33554432; ++ i) {
					buf[i % MEM_LENGTH] = i;
				}

				int sum = 0;
				for (int i = 0; i < MEM_LENGTH; ++ i) {
					int m = buf[i % MEM_LENGTH];
					sum += m;
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
		MemoryXLargeTask task = new MemoryXLargeTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
