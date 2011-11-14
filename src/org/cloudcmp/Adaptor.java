package org.cloudcmp;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cloudcmp.adaptors.*;
import org.cloudcmp.store.*;

/**
 * The base class of all cloud adaptors. 
 * Includes the default implementation of some functions
 * TODO: native functions and traceroute
 * @author Ang Li 
 */
public class Adaptor {
	/**
	 * Configurations of the Adaptor
	 */
	public Map<String, String> configs;
	
	/**
	 * Get an adaptor by name. Note the obtained adaptor may need to be configured.
	 * @param name
	 * @return
	 */
	public static Adaptor getAdaptorByName(String name) {
		if (name.equals("Amazon")) return new AmazonAdaptor();
		if (name.equals("Google")) return new GoogleAdaptor();
		if (name.equals("Microsoft")) return new MicrosoftAdaptor();
		if (name.equals("Rackspace")) return new RackspaceAdaptor();
		return null;
	}
	
	/**
	 * List the available adaptors
	 * @return a list of adaptor names (can be used by getAdaptorByName())
	 */
	public static List<String> listAdaptors() {
		List<String> adaptors = new ArrayList<String>();
		adaptors.add("Amazon");
		adaptors.add("Google");
		adaptors.add("Microsoft");
		adaptors.add("Rackspace");
		return adaptors;
	}
	
	/**
	 * Get the name of the adaptor
	 * @return adaptor name
	 */
	public String getName() {
		return "Generic";
	}
	
	/**
	 * Load configurations from a property file
	 * 
	 * @param filename
	 */
	public void loadConfigFromFile(String filename) {
		// TODO
	}
	
	/**
	 * Get the names of the configurable items
	 * 
	 * @return A list of configurable items
	 */
	public List<String> getConfigItems() {
		List<String> items = new ArrayList<String>();
		items.add("resource_dir");
		items.add("latency_port");
		items.add("bw_port");
		return items;
	}
	
	public Adaptor() {
		configs = new HashMap<String, String>();
		/* default configurations go here */
		String classPath = System.getProperty("java.class.path", ".");
		String [] pathArray = classPath.split(System.getProperty("path.separator"));
		File classDir = null;
		for (String path: pathArray) {
			if (path.endsWith("bootstrap.jar")) {
				File bootstrapPath = new File(path);
				classDir = new File(bootstrapPath.getParent(), "../webapps/cloudcmp/resources");
			}
		}
		
		if (classDir != null) {
			try {
				configs.put("resource_dir", classDir.getCanonicalPath());
			} catch (IOException e) {
				configs.put("resource_dir", "./resources");				
			}
		}
		else configs.put("resource_dir", "./resources"); // works for appengine
		configs.put("latency_port", "25000");
		configs.put("bw_port", "25001");
	}
	
	/* Computation related functions */
	/**
	 * Whether multithreading is supported (some PaaS do not have this feature)
	 * @return true -- multithreading supported
	 */
	public boolean supportMultipleThreads() {
		return true;
	}
	
	/**
	 * Get the computation cost. Don't use the absolute value. Calculate the difference.
	 * @return cost in cents
	 */
	public double getComputeCost() {
		return 0;
	}
	
	/* Storage related functions */
	public boolean hasTableStore() {
		return false;
	}
    public void tableCreate(String tableName) throws IOException {}
    public void tableDelete(String tableName) throws IOException {}
    public void tableInsertRow(String tableName, Row row) throws IOException {}
    public void tableInsertRows(String tableName, List<Row> rows) throws IOException {}
    public void tableUpdateRow(String tableName, String rowId, List<Column> columns) throws IOException {}
    public Row tableGetRow(String tableName, String rowId) throws IOException {return null;}
    public Column tableGetColumn(String tableName, String rowId, String columnName) throws IOException {return null;}
    public List<Row> tableQuery(String tableName, List<Condition> conditions, Order order, int limit) throws IOException {return null;}
    public int tableCount(String tableName, List<Condition> conditions, Order order, int limit) throws IOException {return 0;}
    public void tableDeleteRow(String tableName, String rowId) throws IOException {}

    public boolean hasBlobStore() {
    	return false;
    }
    public void blobCreateContainer(String containerName) throws IOException {}
    public void blobDeleteContainer(String containerName) throws IOException {}
    public void blobUpload(String containerName, String blobName, byte [] bytes) throws IOException {}
    public byte [] blobDownload(String containerName, String blobName, boolean bmark) throws IOException {return null;}
    public void blobDelete(String containerName, String blobName) throws IOException {}
    public boolean blobDoesExist(String containerName, String blobName) throws IOException {return false;}
    
    public boolean hasQueueStore() {
    	return false;
    }
    public void queueCreate(String queueName) throws IOException {}
    public void queueDelete(String queueName) throws IOException {}
    public void queueClear(String queueName) throws IOException {}
    public void queuePutMessage(String queueName, String message) throws IOException {}
    public String queueGetMessage(String queueName) throws IOException {return null;}
    public void queueDeleteLastMessage(String queueName) throws IOException {}
    /**
     * Get the monetary cost of the last storage operation 
     * @return cost in cents
     */
    public double getLastOperationCost() {return 0;}

    /* Network functions */
    public boolean hasNetwork() {
    	return true;
    }
    /**
     * Measure network latency
     * 
     * @param targetAddr	Target address
     * @return latency in microseconds
     * @throws IOException
     */
    public long measureLatency(String targetAddr) throws IOException {
    	/* by default, we use TCP ping as it is supported by all platforms. */
        int targetPort;
        if (!configs.containsKey("latency_port")) {
        	return -1;            
        }
        targetPort = Integer.parseInt(configs.get("latency_port"));

        InetSocketAddress sockAddr = new InetSocketAddress(targetAddr, targetPort);

        // try connect
        Socket s = new Socket();
        long startTime = System.nanoTime();
        long duration;
        try {
            s.connect(sockAddr);
            duration = (System.nanoTime() - startTime) / 1000; // in microseconds
            s.close();
        }
        catch (IOException ex) {
            throw ex;
        }
        catch (IllegalArgumentException ex) {
            return -1;
        }

        return duration;
    }
    
    /**
     * Measure TCP bandwidth
     * 
     * @param targetAddr	Target address
     * @param transferSize	Transfer size, in unit of MB, must be multiples of 8MB
     * @return bandwidth in bps
     * @throws IOException
     */
    public long measureBandwidth(String targetAddr, long transferSize) throws IOException {
    	int targetPort;
    	if (!configs.containsKey("bw_port")) {
    		return -1;
    	}
    	
    	targetPort = Integer.parseInt(configs.get("bw_port"));    	
    	
    	InetSocketAddress sockAddr = new InetSocketAddress(targetAddr, targetPort);
    	
    	Socket s = new Socket();
    	s.setTcpNoDelay(true);
    	s.setSendBufferSize(8000000); // 8MB send buffer, enough for wide-area transfer
    	byte [] buffer = new byte[8000000];
    	long startTime;
    	long duration;
    	try {
    		s.connect(sockAddr);
    		long transferred = 0;
    		InputStream is = s.getInputStream();
    		OutputStream os = s.getOutputStream();
    		
    		byte [] transferSizeBytes = ByteBuffer.allocate(8).putLong(transferSize).array();    		
    		os.write(transferSizeBytes); // first tell the destination how many bytes we are going to send
    		os.flush();
    		
    		startTime = System.nanoTime();
    		while (transferred < transferSize) {
    			os.write(buffer);
    			transferred += buffer.length;
    		}
    		os.flush();
    		is.read(); // waiting for all bytes to be received by the destination
    		duration = System.nanoTime() - startTime;
    		s.close();
    	}
    	catch (IOException ex) {
    		throw ex;
    	}
    	catch (Exception ex) {
    		ex.printStackTrace();
    		return -1;
    	}
    	
    	return (long)(transferSize * 8 / ((double)duration / 1000000000));
    }
    
    /* Management functions */
    public String getExternalIP() {
    	try {
    		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
    		for (NetworkInterface netIf : Collections.list(nets)) {
    			Enumeration<InetAddress> addresses = netIf.getInetAddresses();
    			for (InetAddress addr : Collections.list(addresses)) {
    				String addrStr = addr.getHostAddress();
    				if (addrStr.indexOf('.') == -1) continue;
    				if (addrStr.indexOf("127.") == 0 || addrStr.indexOf("10.") == 0 
    						|| addrStr.indexOf("192.168.") == 0 || addrStr.indexOf("169.254.") == 0) continue;
    				return addrStr;
    			}
    		}
    	}
    	catch (Exception ex) {
    		return null;
    	}
    	return null;
    }
    public String getInternalIP() {
    	try {
    		Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
    		for (NetworkInterface netIf : Collections.list(nets)) {
    			Enumeration<InetAddress> addresses = netIf.getInetAddresses();
    			for (InetAddress addr : Collections.list(addresses)) {
    				String addrStr = addr.getHostAddress();
    				if (addrStr.indexOf("10.") == 0 || addrStr.indexOf("192.168.") == 0 || addrStr.indexOf("169.254.") == 0) return addrStr;
    			}
    		}
    	}
    	catch (Exception ex) {
    		return null;
    	}
    	return null;    	
    }
}