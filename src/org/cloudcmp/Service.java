package org.cloudcmp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.services.BandwidthService;
import org.cloudcmp.services.LatencyService;

/**
 * The base class of all services
 * @author auron
 *
 */
public class Service implements Runnable {
	static private Map<String, Class<? extends Service>> serviceMap = getAllServices();
	static private Map<String, Class<? extends Service>> getAllServices() {
		List<Class<? extends Service>> services = new ArrayList<Class<? extends Service>>();
		services.add(BandwidthService.class);
		services.add(LatencyService.class);
				
		Map<String, Class<? extends Service>> serviceMap = new HashMap<String, Class<? extends Service>>();		
		for (Class<? extends Service> c: services) {
			try {
				Service s = (Service)c.getDeclaredConstructor(Adaptor.class).newInstance((Object)null);
				serviceMap.put(s.getServiceName(), c);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return serviceMap;		
	}
	
	static public List<String> listServices(Adaptor adaptor) {
		List<String> serviceNames = new ArrayList<String>();
		for (String serviceName: serviceMap.keySet()) {
			Class<? extends Service> c = serviceMap.get(serviceName);
			try {
				Service s = (Service)c.getDeclaredConstructor(Adaptor.class).newInstance((Object)adaptor);
				if (s.requiresBlobStore() && !adaptor.hasBlobStore()) continue;
				if (s.requiresTableStore() && !adaptor.hasTableStore()) continue;
				if (s.requiresQueueStore() && !adaptor.hasQueueStore()) continue;
				if (s.requiresNetwork() && !adaptor.hasNetwork()) continue;
				serviceNames.add(s.getServiceName());
			}
			catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		return serviceNames;
	}
	
	static public Service getServiceByName(String serviceName, Adaptor adaptor) {
		Class<? extends Service> c = serviceMap.get(serviceName);
		if (c == null) return null;
		try {
			Service s = (Service)c.getDeclaredConstructor(Adaptor.class).newInstance((Object)adaptor);
			return s;
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}		
	}
	
	public static List<Thread> runningServiceThreads = new ArrayList<Thread>();
	public static List<Service> runningServices = new ArrayList<Service>();
	
	protected Adaptor adaptor;
	
	public String getServiceName() {
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
	
	public Service(Adaptor adaptor) {
		this.adaptor = adaptor;
	}
	
	@Override
	public void run() {}
}
