/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */
package org.cloudcmp.tasks.compute.serial;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.BitSet;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;

public class SerialTask extends ComputeTask {
	public SerialTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}
	
	public String getTaskName() {
		return "serial";
	}
	
	protected SingleComputeTask getSingleComputeTask() {		
		return new SingleComputeTask();
	}

	static boolean doEquals=true;

	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		public Object[][] instances;
		public ByteArrayOutputStream[] streams;
		public Object[] threadInstances;
		ByteArrayOutputStream bos;

		public void setupBenchmark(){    	
			int threads = 1;
			instances = new Object[threads][Utils.classesNumber];
			streams = new ByteArrayOutputStream[threads];       
			try {      
				for (int i = 0; i < threads; i ++) {          	
					instances[i] = Utils.createInstances();
					streams[i] = new ByteArrayOutputStream() {
						public synchronized byte[] toByteArray() {
							return buf;
						}
					};
				}    
			} catch (Exception e) {
				e.printStackTrace();
			}
		}


		public void serialize() throws Exception {    	
			BitSet result = Utils.createBitSet();
			bos.reset();
			ObjectOutputStream oos = new ObjectOutputStream(bos);    	
			for (int i = 0; i < Utils.singleLoop; i ++) {
				for (int j = 0; j < threadInstances.length; j ++) {
					oos.writeObject(threadInstances[j]);
				}
				oos.flush();
				oos.reset();
			}   	

			ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray(), 0, bos.size());
			ObjectInputStream ois = new ObjectInputStream(bis);
			for (int i = 0; i < Utils.singleLoop; i ++) {    		
				for (int j = 0; j < threadInstances.length; j ++) {
					Object obj = ois.readObject();   
					if (doEquals) {   	    		
						result.set(j, result.get(j) && obj.equals(threadInstances[j]));
					}    
				}

			}
			oos.close();
			ois.close();    	
		}	
		
		public void realRun() {
			setupBenchmark();
			try {
				serialize();
				serialize();
				serialize();
			} catch (Exception e) {
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
		SerialTask task = new SerialTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
