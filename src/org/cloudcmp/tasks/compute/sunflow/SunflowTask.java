/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 *
 * Copyright (c) 2007 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */

package org.cloudcmp.tasks.compute.sunflow;

import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;
import org.sunflow.Benchmark;
import org.sunflow.system.UI;
import org.sunflow.system.UI.Module;
import org.sunflow.system.UI.PrintLevel;

public class SunflowTask extends ComputeTask {
	public SunflowTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}

	public String getTaskName() {
		return "sunflow";
	}
	
	protected SingleComputeTask getSingleComputeTask() {		
		return new SingleComputeTask();
	}
	
	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		public static final int resolution = 128;
		private Benchmark[] benchmarks;   

		class BenchmarkImpl extends Benchmark{
			public BenchmarkImpl(int resolution, boolean showOutput, boolean showBenchmarkOutput,
					boolean saveOutput, int threads) {
				super(resolution, showOutput, showBenchmarkOutput, saveOutput, threads);
			}

			public void print(Module m, PrintLevel level, String s) {
			}
		}

		Benchmark benchmark;

		public void setupBenchmark() { 
			int threads = 1;
			int bmThreads = 1;
			benchmarks = new Benchmark[bmThreads];
			for (int i = 0; i < bmThreads; i ++) {        	
				benchmarks[i] = new BenchmarkImpl(resolution, false, true, false, threads);
				benchmarks[i].kernelBegin();
			}
		}
		
		public void realRun() {
			benchmark = new BenchmarkImpl(resolution, false, true, false, 1);
			benchmark.kernelBegin();
			benchmark.kernelMain();
			benchmark.kernelEnd();
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
		SunflowTask task = new SunflowTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
