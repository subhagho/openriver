/**
 * 
 */
package com.wookler.server.river.test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.UUID;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

/**
 * @author subho
 *
 */
public class InputDataGenerator {
	public static final class Args {
		@Option(name = "--outfile", usage = "Output filename to be generated.", required = true)
		private File outfile;

		@Option(name = "--msize", usage = "Message size.", required = false)
		private int mSize = 1024;

		@Option(name = "--mcount", usage = "Number of messages to generate.", required = false)
		private int mCount = 100000;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		CmdLineParser parser = null;
		try {
			Args a = new Args();
			parser = new CmdLineParser(a);
			parser.parseArgument(args);

			FileOutputStream fos = new FileOutputStream(a.outfile);
			try {
				for (int ii = 0; ii < a.mCount; ii++) {
					String mesg = generateMessage(a.mSize);
					fos.write(mesg.getBytes("UTF-8"));
				}
			} finally {
				fos.close();
			}
		} catch (CmdLineException e) {
			System.err.println(e.getMessage());
			if (parser != null)
				parser.printUsage(System.err);
			return;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static String generateMessage(int size) {
		StringBuffer buff = new StringBuffer(UUID.randomUUID().toString()).append("\t");
		int count = 0;
		while (buff.length() < size) {
			if (count % 3 == 0)
				buff.append("{param_").append(count).append("=").append(UUID.randomUUID().toString()).append("}");
			else
				buff.append("{param_").append(count).append("=").append(System.currentTimeMillis()).append("}");
			count++;
		}
		buff.append("\n");
		return buff.toString();
	}
}
