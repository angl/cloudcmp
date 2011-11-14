/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */

package org.cloudcmp.tasks.compute.crypto.aes;

import java.security.AlgorithmParameters;
import java.security.SecureRandom;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;
import org.cloudcmp.tasks.compute.crypto.Util;

public class CryptoAESTask extends ComputeTask {
	private String resourcePath;
	
	public CryptoAESTask(Adaptor adaptor) {
		super(adaptor);
		if (adaptor != null) {
			resourcePath = adaptor.configs.get("resource_dir");
		}
	}

	public String getTaskName() {
		return "crypto.aes";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}

	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		public final static boolean DEBUG = false;

		final static int aesKeySize = 128;
		final static int desKeySize = 168;
		final static int level = 12;

		SecretKey aesKey = null;
		SecretKey desKey = null;

		KeyGenerator aesKeyGen = null;
		KeyGenerator desKeyGen = null;

		AlgorithmParameters algorithmParameters = null;

		private void printMe(String name, byte [] arr) {
			System.out.print("  " + name + ":");
			for (int i = 0; i < arr.length; i++) {
				System.out.print(arr[i]);
			}
			System.out.println();
		}

		/**
		 * Will encrypt the indata level number of times.
		 * @param indata Data to encrypt.
		 * @param key Key to use for encryption.
		 * @param algorithm Algorithm/Standard to use.
		 * @param level Number of times to encrypt.
		 * @return The encrypted version of indata.
		 */
		private byte[] encrypt(byte [] indata, SecretKey key, String algorithm, int level) {

			if (DEBUG) printMe("indata", indata);

			byte[] result = indata;

			try {
				Cipher c = Cipher.getInstance(algorithm);
				c.init(Cipher.ENCRYPT_MODE, key);
				algorithmParameters = c.getParameters();

				for (int i = 0; i < level; i++) {
					byte[] r1 = c.update(result);
					byte[] r2 = c.doFinal();

					if (DEBUG) printMe("[" + i + "] r1", r1);
					if (DEBUG) printMe("[" + i + "] r2", r2);

					result = new byte[r1.length + r2.length];
					System.arraycopy(r1, 0, result, 0, r1.length);
					System.arraycopy(r2, 0, result, r1.length, r2.length);
				}
			} catch (Exception e) {
			}

			if (DEBUG) printMe("result", result);
			return result;
		}

		/**
		 * Will decrypt the indata level number of times.
		 * @param indata Data to decrypt.
		 * @param key Key to use for encryption.
		 * @param algorithm
		 * @param level
		 * @return
		 */
		private byte[] decrypt(byte[] indata, SecretKey key, String algorithm, int level) {

			if (DEBUG) printMe("indata", indata);

			byte[] result = indata;

			try {
				Cipher c = Cipher.getInstance(algorithm);
				c.init(Cipher.DECRYPT_MODE, key, algorithmParameters);

				for (int i = 0; i < level; i++) {
					byte[] r1 = c.update(result);
					byte[] r2 = c.doFinal();
					if (DEBUG) printMe("[" + i + "] r1", r1);
					if (DEBUG) printMe("[" + i + "] r2", r2);

					result = new byte[r1.length + r2.length];
					System.arraycopy(r1, 0, result, 0, r1.length);
					System.arraycopy(r2, 0, result, r1.length, r2.length);
				}

			} catch (Exception e) {
			}

			if (DEBUG) printMe("result", result);
			return result;
		}

		public void runEncryptDecrypt(SecretKey key, String algorithm, String inputFile) {
			// for debug
			byte [] indata = Util.getTestData(inputFile);
			byte [] cipher = encrypt(indata, key, algorithm, level);
			byte [] plain = decrypt(cipher, key, algorithm, level);
			boolean match = Util.check(indata, plain);
		}

		public long runTask() {
			long startTime = System.currentTimeMillis();
			setupBenchmark();
			runEncryptDecrypt(aesKey, "AES/CBC/NoPadding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(aesKey, "AES/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(desKey, "DESede/CBC/NoPadding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(desKey, "DESede/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(aesKey, "AES/CBC/NoPadding", resourcePath + Util.TEST_DATA_2);
			runEncryptDecrypt(aesKey, "AES/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_2);
			runEncryptDecrypt(desKey, "DESede/CBC/NoPadding", resourcePath + Util.TEST_DATA_2);
			runEncryptDecrypt(desKey, "DESede/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_2);
			return System.currentTimeMillis() - startTime;
		}

		public void setupBenchmark() {
			try {
				byte [] seed =  {0x4, 0x7, 0x1, 0x1};
				SecureRandom random = new SecureRandom(seed);
				aesKeyGen = KeyGenerator.getInstance("AES");
				aesKeyGen.init(aesKeySize, random);
				desKeyGen = KeyGenerator.getInstance("DESede");
				desKeyGen.init(desKeySize, random);
				aesKey = aesKeyGen.generateKey();
				desKey = desKeyGen.generateKey();
			} catch (Exception e) {
			}
		}
		
		public void realRun() {
			setupBenchmark();
			runEncryptDecrypt(aesKey, "AES/CBC/NoPadding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(aesKey, "AES/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(desKey, "DESede/CBC/NoPadding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(desKey, "DESede/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_1);
			runEncryptDecrypt(aesKey, "AES/CBC/NoPadding", resourcePath + Util.TEST_DATA_2);
			runEncryptDecrypt(aesKey, "AES/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_2);
			runEncryptDecrypt(desKey, "DESede/CBC/NoPadding", resourcePath + Util.TEST_DATA_2);
			runEncryptDecrypt(desKey, "DESede/CBC/PKCS5Padding", resourcePath + Util.TEST_DATA_2);
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
		CryptoAESTask task = new CryptoAESTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
