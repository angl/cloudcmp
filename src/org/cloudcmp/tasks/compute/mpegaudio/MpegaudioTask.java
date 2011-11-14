/*
 * Copyright (c) 2008 Standard Performance Evaluation Corporation (SPEC)
 *               All rights reserved.
 *
 * Copyright (c) 1997,1998 Sun Microsystems, Inc. All rights reserved.
 *
 * This source code is provided as is, without any express or implied warranty.
 */
package org.cloudcmp.tasks.compute.mpegaudio;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;
import java.util.zip.CRC32;

import org.cloudcmp.Adaptor;
import org.cloudcmp.tasks.compute.ComputeTask;

import javazoom.jl.decoder.Bitstream;
import javazoom.jl.decoder.BitstreamException;
import javazoom.jl.decoder.Decoder;
import javazoom.jl.decoder.DecoderException;
import javazoom.jl.decoder.Header;
import javazoom.jl.decoder.SampleBuffer;

public class MpegaudioTask extends ComputeTask {	
	public MpegaudioTask(Adaptor adaptor) {
		super(adaptor);
		// TODO Auto-generated constructor stub
	}

	public String getTaskName() {
		return "mpegaudio";
	}
	
	protected SingleComputeTask getSingleComputeTask() {
		return new SingleComputeTask();
	}	

	protected class SingleComputeTask extends ComputeTask.SingleComputeTask {
		static final int TRACKS_NUMBER = 6;
		static final int FRAMES_LIMIT = 8000;

		long[] result = new long[TRACKS_NUMBER];

		private String getName(int index) {
			return adaptor.configs.get("resource_dir") + "/mpegaudio/input/track" + index + ".mp3";
		}
		
		public void realRun() {
			try {
				for (int i = 0; i < TRACKS_NUMBER; i++) {
					int ind = i % TRACKS_NUMBER;
					result[ind] = decode(getName(ind));
				}
			} catch (Exception e) {
			}
		}
		

		private void updateCRC32(CRC32 crc32, short[] buffer) {
			int length = buffer.length;
			byte[] b = new byte[length * 2];
			for (int i = 0; i < length; i++) {
				short value = buffer[i];
				b[i] = (byte) buffer[i];
				b[i + length] = (byte) ((value & 0xff00) >> 8);
			}

			crc32.update(b, 0, b.length);
		}

		public long decode(final String name) throws BitstreamException,
		       DecoderException, FileNotFoundException {
			       Bitstream stream = new Bitstream(new FileInputStream(name));
			       Decoder decoder = new Decoder();
			       Header h;
			       CRC32 crc = new CRC32();
			       int decodedFrames = 0;
			       while (decodedFrames < FRAMES_LIMIT && (h = stream.readFrame()) != null) {
				       decodedFrames++;
				       updateCRC32(crc, ((SampleBuffer) decoder.decodeFrame(h, stream))
						       .getBuffer());
				       stream.closeFrame();
			       }
			       stream.close();
			       return crc.getValue();
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
		MpegaudioTask task = new MpegaudioTask(adaptor);
		task.configs.put("num_threads", args[2]);
		task.configs.put("num_runs", args[3]);
		Map<String, String> results = task.run();
		ComputeTask.printResults(results);
	}
}
