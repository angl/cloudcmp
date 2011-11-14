package org.cloudcmp.tasks.compute;

import java.io.*;
import java.util.*;

// a lightweight config class
public class Config {
	public static String resourcePath = null;
	public static String readWritePath = null;
	public static PrintWriter outputWriter = null; // for debugging

	static {
		// get the resourcePath from class path
		String cp = System.getProperty("java.class.path", ".");
		String os = System.getProperty("os.name");
		String [] paths;

		if (os.startsWith("Windows")) {
			paths = cp.split(";");
		}
		else {
			paths = cp.split(":");
		}

		for (String path : paths) {
			if (path.endsWith(".jar")) continue;
			File f = new File(path, "resources");

			if (f.exists()) {
				resourcePath = path;
				readWritePath = path;
				break;
			}
		}

		if (resourcePath == null) {
			System.err.println("Warning: cannot find resources directory");
			resourcePath = ".";
			readWritePath = ".";
		}
	}

	//public static String environment = "tomcat";
}
