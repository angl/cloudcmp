/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 * All rights reserved.
 *
 * Copyright (c) 1997,1998 Sun Microsystems, Inc. All rights reserved.
 *
 * Modified by Kaivalya M. Dixit & Don McCauley (IBM) to read input files This
 * source code is provided as is, without any express or implied warranty.
 */

package org.cloudcmp.tasks.compute.compress;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;

public class CompressTask extends ComputeTask {
	public CompressTask(Adaptor adaptor) {
		super(adaptor);
		if (adaptor != null) {
			String resourcePath = adaptor.configs.get("resource_dir");
			fileNames.add(resourcePath + "/compress/input/202.tar");
			fileNames.add(resourcePath + "/compress/input/205.tar");
			fileNames.add(resourcePath + "/compress/input/208.tar");
			fileNames.add(resourcePath + "/compress/input/209.tar");
			fileNames.add(resourcePath + "/compress/input/210.tar");
			fileNames.add(resourcePath + "/compress/input/211.tar");
			fileNames.add(resourcePath + "/compress/input/213x.tar");
			fileNames.add(resourcePath + "/compress/input/228.tar");
			fileNames.add(resourcePath + "/compress/input/239.tar");
			fileNames.add(resourcePath + "/compress/input/misc.tar");
		}
	}
	
	public String getTaskName() {
		return "compress";
	}

	public List<String> fileNames = new ArrayList<String>();
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}

	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		public static final int LOOP_COUNT = 2;
		public Source[] SOURCES;
		public byte[][] COMPRESS_BUFFERS;
		public byte[][] DECOMPRESS_BUFFERS;
		public Compress CB;
		
		public int MAX_LENGTH;
		
		public void realRun() {
			prepareBuffers();
			for (int i = 0; i < LOOP_COUNT; i++) {
				for (int j = 0; j < fileNames.size(); j++) {
					Source source = SOURCES[j];
					OutputBuffer comprBuffer, decomprBufer;
					comprBuffer = CB.performAction(source.getBuffer(),
							source.getLength(),
							CB.COMPRESS,
							COMPRESS_BUFFERS[0]);
					decomprBufer = CB.performAction(COMPRESS_BUFFERS[0],
							comprBuffer.getLength(),
							CB.UNCOMPRESS,
							DECOMPRESS_BUFFERS[0]);
				}
			}	
		}
		
		void prepareBuffers() {
			CB = new Compress();
			SOURCES = new Source[fileNames.size()];
			for (int i = 0; i < fileNames.size(); i ++) {
				SOURCES[i] = new Source(fileNames.get(i));
			}
			DECOMPRESS_BUFFERS = new byte[1][MAX_LENGTH];
			COMPRESS_BUFFERS = new byte[1][MAX_LENGTH];
		}

		public class Source {
			private byte[] buffer;
			private long crc;
			private int length;

			public Source(String fileName) {
				buffer = fillBuffer(fileName);
				length = buffer.length;
				MAX_LENGTH = Math.max(length, MAX_LENGTH);
				CRC32 crc32 = new CRC32();
				crc32.update(buffer, 0, length);
				crc = crc32.getValue();
			}

			long getCRC() {
				return crc;
			}

			int getLength() {
				return length;
			}

			byte[] getBuffer() {
				return buffer;
			}

			private byte[] fillBuffer(String fileName) {
				try {
					FileInputStream sif = new FileInputStream(fileName);
					int length = (int) new File(fileName).length();
					int counter = 0;

					// Only allocate size of input file rather than MAX - kmd
					// If compressed file is larger than input file this allocation
					// will fail and out of bound exception will occur
					// In real lie, compress will no do any compression as no
					// space is saved.-- kaivalya
					byte[] result = new byte[length];

					int bytes_read;
					while ((bytes_read = sif.read(result, counter,
									(length - counter))) > 0) {
						counter += bytes_read;
					}

					sif.close(); // release resources

					if (counter != length) {
						System.out.println(
								"ERROR reading test input file");
					}
					
					// for debug			
					return result;
				} catch (IOException e) {
				}

				return null;
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
		CompressTask task = new CompressTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}

