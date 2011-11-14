package org.cloudcmp;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.tasks.compute.compress.CompressTask;
import org.cloudcmp.tasks.compute.crypto.aes.CryptoAESTask;
import org.cloudcmp.tasks.compute.crypto.rsa.CryptoRSATask;
import org.cloudcmp.tasks.compute.crypto.signverify.CryptoSignverifyTask;
import org.cloudcmp.tasks.compute.memory.large.MemoryLargeTask;
import org.cloudcmp.tasks.compute.memory.small.MemorySmallTask;
import org.cloudcmp.tasks.compute.memory.xlarge.MemoryXLargeTask;
import org.cloudcmp.tasks.compute.mpegaudio.MpegaudioTask;
import org.cloudcmp.tasks.compute.raw.RawTask;
import org.cloudcmp.tasks.compute.scimark.fft.ScimarkFFTTask;
import org.cloudcmp.tasks.compute.scimark.lu.ScimarkLUTask;
import org.cloudcmp.tasks.compute.scimark.monte_carlo.ScimarkMonteCarloTask;
import org.cloudcmp.tasks.compute.scimark.sor.ScimarkSORTask;
import org.cloudcmp.tasks.compute.scimark.sparse.ScimarkSparseTask;
import org.cloudcmp.tasks.compute.serial.SerialTask;
import org.cloudcmp.tasks.compute.sunflow.SunflowTask;
import org.cloudcmp.tasks.network.BandwidthTask;
import org.cloudcmp.tasks.network.GetAddressTask;
import org.cloudcmp.tasks.network.LatencyTask;
import org.cloudcmp.tasks.store.BlobGetTask;
import org.cloudcmp.tasks.store.BlobPutTask;
import org.cloudcmp.tasks.store.BlobTTCTask;
import org.cloudcmp.tasks.store.PopulateBlobTask;
import org.cloudcmp.tasks.store.PopulateQueueTask;
import org.cloudcmp.tasks.store.PopulateTableTask;
import org.cloudcmp.tasks.store.QueueGetTask;
import org.cloudcmp.tasks.store.QueuePutTask;
import org.cloudcmp.tasks.store.QueueTTCTask;
import org.cloudcmp.tasks.store.TableGetTask;
import org.cloudcmp.tasks.store.TablePutTask;
import org.cloudcmp.tasks.store.TableQueryTask;
import org.cloudcmp.tasks.store.TableTTCTask;

/***
 * The base class of all tasks. Every task can be either run as a standalone application, 
 * or from the web interface. Some long running ones can also be triggered in async.
 * @author Ang Li
 *
 */
public class Task {	
	static private Map<String, Class<? extends Task>> taskMap = getAllTasks();
	static private Map<String, Class<? extends Task>> getAllTasks() {
		List<Class<? extends Task>> tasks = new ArrayList<Class<? extends Task>>();
		tasks.add(CompressTask.class);
		tasks.add(CryptoAESTask.class);
		tasks.add(CryptoRSATask.class);
		tasks.add(CryptoSignverifyTask.class);
		tasks.add(MemoryLargeTask.class);
		tasks.add(MemorySmallTask.class);
		tasks.add(MemoryXLargeTask.class);
		tasks.add(MpegaudioTask.class);
		tasks.add(RawTask.class);
		tasks.add(ScimarkFFTTask.class);
		tasks.add(ScimarkLUTask.class);
		tasks.add(ScimarkMonteCarloTask.class);
		tasks.add(ScimarkSORTask.class);
		tasks.add(ScimarkSparseTask.class);
		tasks.add(SerialTask.class);
		tasks.add(SunflowTask.class);
		tasks.add(BlobGetTask.class);
		tasks.add(BlobPutTask.class);
		tasks.add(BlobTTCTask.class);
		tasks.add(PopulateBlobTask.class);
		tasks.add(PopulateQueueTask.class);
		tasks.add(PopulateTableTask.class);
		tasks.add(QueueGetTask.class);
		tasks.add(QueuePutTask.class);
		tasks.add(QueueTTCTask.class);
		tasks.add(TableGetTask.class);
		tasks.add(TablePutTask.class);
		tasks.add(TableQueryTask.class);
		tasks.add(TableTTCTask.class);
		tasks.add(BandwidthTask.class);
		tasks.add(GetAddressTask.class);
		tasks.add(LatencyTask.class);
		
		Map<String, Class<? extends Task>> taskMap = new HashMap<String, Class<? extends Task>>();		
		for (Class<? extends Task> c: tasks) {
			try {
				Task t = (Task)c.getDeclaredConstructor(Adaptor.class).newInstance((Object)null);
				taskMap.put(t.getTaskName(), c);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return taskMap;		
	}
	
	static public List<String> listTasks(Adaptor adaptor) {
		List<String> taskNames = new ArrayList<String>();
		for (String taskName: taskMap.keySet()) {
			Class<? extends Task> c = taskMap.get(taskName);
			try {
				Task t = (Task)c.getDeclaredConstructor(Adaptor.class).newInstance((Object)adaptor);
				if (t.requiresBlobStore() && !adaptor.hasBlobStore()) continue;
				if (t.requiresTableStore() && !adaptor.hasTableStore()) continue;
				if (t.requiresQueueStore() && !adaptor.hasQueueStore()) continue;
				if (t.requiresNetwork() && !adaptor.hasNetwork()) continue;
				taskNames.add(t.getTaskName());
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		return taskNames;		
	}
	
	static public Task getTaskByName(String taskName, Adaptor adaptor) {
		Class<? extends Task> c = taskMap.get(taskName);
		if (c == null) return null;
		try {
			Task t = (Task)c.getDeclaredConstructor(Adaptor.class).newInstance((Object)adaptor);
			return t;
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}		
	}
	
	/**
	 * Configurations of the task
	 */
	public Map<String, String> configs;
	
	protected Adaptor adaptor;
	
	public Task(Adaptor adaptor) {
		this.adaptor = adaptor;
		configs = new HashMap<String, String>();
	}
	
	/**
	 * Get a string representation of the task
	 * @return the task's name
	 */
	public String getTaskName() {
		return null;
	}
	
	/**
	 * Whether table store is required to run the task
	 * @return
	 */
	public boolean requiresTableStore() {
		return false;
	}
	
	/**
	 * Whether blob store is required to run the task
	 * @return
	 */
	public boolean requiresBlobStore() {
		return false;
	}
	
	/**
	 * Whether queue store is required to run the task
	 * @return
	 */
	public boolean requiresQueueStore() {
		return false;
	}
	
	/**
	 * Whether network is required to run the task
	 * @return
	 */
	public boolean requiresNetwork() {
		return false;
	}
	
	/**
	 * Get the available options for the task
	 * @return A list of options
	 */
	public List<String> getConfigItems() {
		List<String> items = new ArrayList<String>();
		return items;
	}
	
	/**
	 * Whether async execution is supported
	 * @return true means async is supported (start, isFinished, retrieveResults are meaningful)
	 */
	public boolean isAsyncSupported() {
		return false;
	}
	
	/**
	 * Synchronously run the task
	 * @return A result set
	 */
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		return results;
	}
	
	/** 
	 * Asynchronously start the task
	 * @return a handle for the running task. -1 means the task failed at startup.
	 */
	public long start() {
		return 0;
	}
	
	/**
	 * Stop a runaway task
	 * @param handle the handle of the task
	 */
	public void stop(long handle) {
		
	}
	
	/**
	 * Check whether a running task has finished
	 * @param handle	the handle returned by start()
	 * @return	true means the task has finished. In this case retrieveResults() can be called to get the task returns.
	 */
	public boolean isFinished(long handle) {
		return true;
	}
	
	/**
	 * Retrieve the task returns
	 * @param handle	the handle returned by start()
	 * @return	A result set
	 */
	public Map<String, String> retrieveResults(long handle) {
		Map<String, String> results = new HashMap<String, String>();
		return results;
	}
	
	protected static void printResults(Map<String, String> results) {
		for (String key: results.keySet()) {
			System.out.println(key + ":" + results.get(key));
		}
	}
}