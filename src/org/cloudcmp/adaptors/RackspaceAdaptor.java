package org.cloudcmp.adaptors;

import java.util.HashMap;
import java.util.List;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.cloudcmp.Adaptor;

import com.rackspacecloud.client.cloudfiles.*;

public class RackspaceAdaptor extends Adaptor {	
	private FilesClient blobClient = null;
	
	public RackspaceAdaptor() {
		super();
		// default configurations are put here
		configs.put("compute_dollar_per_hour", "0.015"); // price of the smallest instance
		configs.put("auth_url", "https://auth.api.rackspacecloud.com/v1.0"); // by default, use the US auth URL
		configs.put("use_internal_IP", "true");
	}
	
	public String getName() {
		return "Rackspace";
	}

	public List<String> getConfigItems() {	
		List<String> items = super.getConfigItems();
		items.add("username");
		items.add("API_access_key");
		items.add("auth_url");
		items.add("compute_dollar_per_hour");
		items.add("use_internal_IP");
		return items;
	}
	
	public double getComputeCost() {
		return (double)(System.currentTimeMillis()) / 36000 * Double.parseDouble(configs.get("compute_dollar_per_hour")) ;
	}
	
	private void initBlobClient() throws IOException {
		blobClient = new FilesClient(configs.get("username"), configs.get("API_access_key"), 
				configs.get("auth_url"), null, 120000);
		boolean useInternalIP = Boolean.parseBoolean(configs.get("use_internal_IP"));
		if (useInternalIP) blobClient.useSnet();		
		try {
			blobClient.login();
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
	}
	
    public boolean hasBlobStore() {
    	return true;
    }
    public void blobCreateContainer(String containerName) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			if (blobClient.containerExists(containerName)) return;
			blobClient.createContainer(containerName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobDeleteContainer(String containerName) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			if (!blobClient.containerExists(containerName)) return;
			blobClient.deleteContainer(containerName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobUpload(String containerName, String blobName, byte [] bytes) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			blobClient.storeObject(containerName, bytes, "application/octet-stream", blobName, new HashMap<String, String>());
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public byte [] blobDownload(String containerName, String blobName, boolean bmark) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			if (bmark) {
				InputStream is = blobClient.getObjectAsStream(containerName, blobName);
				byte buf[] = new byte[8000000];
				int count;
				while ((count = is.read(buf)) > 0) {}
				return buf;
			}
			else 
				return blobClient.getObject(containerName, blobName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public void blobDelete(String containerName, String blobName) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			blobClient.deleteObject(containerName, blobName);
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    public boolean blobDoesExist(String containerName, String blobName) throws IOException {
    	if (blobClient == null) initBlobClient();
		try {
			blobClient.getObjectMetaData(containerName, blobName);
			return true;
		}
		catch (FileNotFoundException ex) {
			return false;
		}
		catch (Exception ex) {
			throw new IOException(ex.getMessage());
		}
    }
    
    public double getLastOperationCost() {
    	return 0; // so far, CloudFiles does not charge based on # of requests.
    }
}
