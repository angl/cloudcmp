package org.cloudcmp.services;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;

import org.cloudcmp.Adaptor;
import org.cloudcmp.Service;

public class LatencyService extends Service {

	public LatencyService(Adaptor adaptor) {
		super(adaptor);
	}
	
	public String getServiceName() {
		return "latencyservice";
	}
	
	public boolean requiresNetwork() {
		return true;
	}
	
	@Override
	public void run() {
		if (!adaptor.hasNetwork()) return;
		
		int listenPort = Integer.parseInt(adaptor.configs.get("latency_port"));
		try {
			ServerSocket serverSocket = new ServerSocket(listenPort);
			serverSocket.setReuseAddress(true);
			while (true) {
				Socket clientSocket = serverSocket.accept();
	    		clientSocket.close();
			}
		} catch (IOException e) {			
			e.printStackTrace();
		}
	}
}
