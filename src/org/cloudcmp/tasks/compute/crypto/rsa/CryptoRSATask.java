/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */

package org.cloudcmp.tasks.compute.crypto.rsa;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Map;

import javax.crypto.Cipher;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;
import org.cloudcmp.tasks.compute.crypto.Util;

public class CryptoRSATask extends ComputeTask {
	private String resourcePath;
	
	public CryptoRSATask(Adaptor adaptor) {
		super(adaptor);
		if (adaptor != null) {
			resourcePath = adaptor.configs.get("resource_dir");
		}
	}
	
	public String getTaskName() {
		return "crypto.rsa";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}

	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {		
		final static int level = 1;

		PublicKey rsaPub;
		PrivateKey rsaPriv;

		public byte [] encrypt(byte[] indata, String algorithm) {

			try {
				Cipher c = Cipher.getInstance(algorithm);
				byte[] result = indata;

				c.init(Cipher.ENCRYPT_MODE, rsaPub);
				result = c.doFinal(result);

				return result;

			} catch (Exception e) {
			}

			return null;
		}

		public byte[] decrypt(byte[] indata, String algorithm) {

			try {
				Cipher c = Cipher.getInstance(algorithm);

				byte[] result = indata;

				c.init(Cipher.DECRYPT_MODE, rsaPriv);
				result = c.doFinal(result);

				return result;

			} catch (Exception e) {
			}

			return null;
		}

		public void runSingleEncryptDecrypt(String algorithm, String inputFile) {
			byte [] indata = Util.getTestData(inputFile);
			byte [] cipher = encrypt(indata, algorithm);
			byte [] plain = decrypt(cipher, algorithm);
			boolean match = Util.check(indata, plain);
		}

		public void runMultiEncryptDecrypt(String algorithm, String inputFile) {
			int blockSize = 64;
			byte [] fullIndata = Util.getTestData(inputFile);
			byte [] indata = new byte[blockSize];
			int pass = 0;
			int fail = 0;
			int check = 0;
			for (int i = 0; i + blockSize < fullIndata.length; i+= blockSize) {
				System.arraycopy(fullIndata, i, indata, 0, blockSize);
				byte [] cipher = encrypt(indata, algorithm);
				byte [] plain = decrypt(cipher, algorithm);
				if (Util.check(indata, plain)) {
					pass++;
					check += Util.checkSum(plain);
				} else {
					fail++;
				}
			}
		}

		public void setupBenchmark() {
			try {
				KeyPairGenerator rsaKeyPairGen = KeyPairGenerator.getInstance("RSA");
				// 512, 768 and 1024 are commonly used
				rsaKeyPairGen.initialize(1024);

				KeyPair rsaKeyPair = rsaKeyPairGen.generateKeyPair();

				rsaPub = rsaKeyPair.getPublic();
				rsaPriv = rsaKeyPair.getPrivate();

			} catch (Exception e) {
			}
		}
		
		public void realRun() {
			setupBenchmark();
			runSingleEncryptDecrypt("RSA/ECB/PKCS1Padding", resourcePath + Util.TEST_DATA_3);
			runMultiEncryptDecrypt("RSA/ECB/PKCS1Padding", resourcePath + Util.TEST_DATA_5);
			// Run some more, in order to increase operation workload.
			runSingleEncryptDecrypt("RSA/ECB/PKCS1Padding", resourcePath + Util.TEST_DATA_3);
			runMultiEncryptDecrypt("RSA/ECB/PKCS1Padding", resourcePath + Util.TEST_DATA_5);
			runSingleEncryptDecrypt("RSA/ECB/PKCS1Padding", resourcePath + Util.TEST_DATA_3);
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
		CryptoRSATask task = new CryptoRSATask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}

}
