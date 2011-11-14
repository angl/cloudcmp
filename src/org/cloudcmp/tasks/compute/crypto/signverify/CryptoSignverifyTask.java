/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */

package org.cloudcmp.tasks.compute.crypto.signverify;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.Signature;
import java.util.Map;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;
import org.cloudcmp.tasks.compute.crypto.Util;

public class CryptoSignverifyTask extends ComputeTask {
	private String resourcePath;
	
	public CryptoSignverifyTask(Adaptor adaptor) {
		super(adaptor);
		if (adaptor != null) {
			resourcePath = adaptor.configs.get("resource_dir");
		}
	}

	public String getTaskName() {
		return "crypto.signverify";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}
	
	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		private PublicKey rsaPub;
		private PrivateKey rsaPriv;

		private PublicKey dsaPub;
		private PrivateKey dsaPriv;

		private final static int iterations = 10;

		public byte [] sign(byte[] indata, String algorithm, PrivateKey privKey) {

			try {
				Signature signature = Signature.getInstance(algorithm);
				signature.initSign(privKey);
				signature.update(indata);
				return signature.sign();
			} catch (Exception e) {
			}

			return null;
		}

		public boolean verify(byte[] indata, String algorithm, byte [] signed, PublicKey pubKey) {

			try {

				Signature signature = Signature.getInstance(algorithm);
				signature.initVerify(pubKey);

				signature.update(indata);

				return signature.verify(signed);

			} catch (Exception e) {
			}

			return false;
		}



		public void runSignVerify(byte[] indata, String algorithm, PrivateKey privKey, PublicKey pubKey) {

			byte [] signed = sign(indata, algorithm, privKey);
			boolean verification = verify(indata, algorithm, signed, pubKey);
		}

		public void setupBenchmark() {

			try {
				KeyPairGenerator rsaKeyPairGen = KeyPairGenerator.getInstance("RSA");
				// 512, 768 and 1024 are commonly used
				rsaKeyPairGen.initialize(1024);

				KeyPair rsaKeyPair = rsaKeyPairGen.generateKeyPair();

				rsaPub = rsaKeyPair.getPublic();
				rsaPriv = rsaKeyPair.getPrivate();

				KeyPairGenerator dsaKeyPairGen = KeyPairGenerator.getInstance("DSA");
				dsaKeyPairGen.initialize(1024);

				KeyPair dsaKeyPair = dsaKeyPairGen.generateKeyPair();

				dsaPub = dsaKeyPair.getPublic();
				dsaPriv = dsaKeyPair.getPrivate();
			} catch (Exception e) {
			}
		}
		
		public void realRun() {
			setupBenchmark();
			for (int i = 0; i < iterations; i++) {
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_4), "MD5withRSA", rsaPriv, rsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_4), "SHA1withRSA", rsaPriv, rsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_4), "SHA1withDSA", dsaPriv, dsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_4), "SHA256withRSA", rsaPriv, rsaPub);

				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_5), "MD5withRSA", rsaPriv, rsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_5), "SHA1withRSA", rsaPriv, rsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_5), "SHA1withDSA", dsaPriv, dsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_5), "SHA256withRSA", rsaPriv, rsaPub);

				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_6), "MD5withRSA", rsaPriv, rsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_6), "SHA1withRSA", rsaPriv, rsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_6), "SHA1withDSA", dsaPriv, dsaPub);
				runSignVerify(Util.getTestData(resourcePath + Util.TEST_DATA_6), "SHA256withRSA", rsaPriv, rsaPub);
			}
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
		CryptoSignverifyTask task = new CryptoSignverifyTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}

}
