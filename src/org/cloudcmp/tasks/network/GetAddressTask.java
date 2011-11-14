package org.cloudcmp.tasks.network;

import java.util.HashMap;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Task;

public class GetAddressTask extends Task {

	public GetAddressTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}
	
	public String getTaskName() {
		return "getaddresstask";
	}
	
	public boolean isAsyncSupported() {
		return false;
	}
	
	public boolean requiresNetwork() {
		return true;
	}
	
	public Map<String, String> run() {
		Map<String, String> results = new HashMap<String, String>();
		String externalIP = adaptor.getExternalIP();
		String internalIP = adaptor.getInternalIP();
		results.put("external_IP", externalIP);
		results.put("internal_IP", internalIP);
		
		return results;
	}
}
