package org.cloudcmp.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Service;

/**
 * TCP bandwidth measurement service.
 * @author Ang Li
 *
 */
public class BandwidthService extends Service {
	public BandwidthService(Adaptor adaptor) {
		super(adaptor);
	}
	
	public String getServiceName() {
		return "bandwidthservice";
	}
	
	public boolean requiresNetwork() {
		return true;
	}

	@Override
	public void run() {
		if (!adaptor.hasNetwork()) return;
				
		int listenPort = Integer.parseInt(adaptor.configs.get("bw_port"));
		try {
			ServerSocket serverSocket = new ServerSocket(listenPort);
			serverSocket.setReuseAddress(true);
			serverSocket.setReceiveBufferSize(8000000);
			byte [] buffer = new byte[8000000];
			while (true) {
				Socket clientSocket = serverSocket.accept();
				clientSocket.setReceiveBufferSize(8000000);
	    		InputStream is = clientSocket.getInputStream();
	    		OutputStream os = clientSocket.getOutputStream();
	    		byte [] transferSizeBytes = new byte[8];
	    		is.read(transferSizeBytes);
	    		long transferSize = ByteBuffer.wrap(transferSizeBytes).getLong();
	    		long transferred = 0;
	    		while (transferred < transferSize) {
	    			transferred += is.read(buffer);	    			
	    		}
	    		os.write(1); // tell the sender we have received everything
	    		os.flush();
	    		clientSocket.close();
			}
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
}
