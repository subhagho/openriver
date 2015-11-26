/*
 * * Copyright 2014 Subhabrata Ghosh
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 */

package com.wookler.server.river.test;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 20/08/14
 */
public class Consumer extends Processor<String>implements Runnable {
	private static final Logger log = LoggerFactory.getLogger(Consumer.class);

	public static final class Constants {
		private static final long READ_TIMEOUT = 1000 * 30; // 30 secs.
	}

	@CParam(name = "directory.output")
	private String				directory;
	private FileOutputStream	writer;
	private int					count	= 0;
	@CParam(name = "processor.sleep", required = false)
	private long				sleept	= -1;
	@CParam(name = "subscriber.pull.name")
	private String				subscriberName;
	private Subscriber<String>	subscriber;
	private int					bcount	= 0;

	private long			r_time	= 0;
	private int				f_index	= 0;
	private int				f_count	= 0;
	private ReentrantLock	f_lock	= new ReentrantLock();

	/**
	 * @return the directory
	 */
	public String getDirectory() {
		return directory;
	}

	/**
	 * @param directory
	 *            the directory to set
	 */
	public void setDirectory(String directory) {
		this.directory = directory;
	}

	/**
	 * @return the sleept
	 */
	public long getSleept() {
		return sleept;
	}

	/**
	 * @param sleept
	 *            the sleept to set
	 */
	public void setSleept(long sleept) {
		this.sleept = sleept;
	}

	/**
	 * @return the subscriberName
	 */
	public String getSubscriberName() {
		return subscriberName;
	}

	/**
	 * @param subscriberName
	 *            the subscriberName to set
	 */
	public void setSubscriberName(String subscriberName) {
		this.subscriberName = subscriberName;
	}

	public Consumer subscriber(Subscriber<String> subscriber) {
		this.subscriber = subscriber;

		return this;
	}

	@Override
	protected ProcessResponse<String> process(List<Message<String>> messages)
			throws ProcessingException, NonFatalProcessorException {
		ProcessResponse<String> resp = new ProcessResponse<String>();
		try {
			long ts = System.currentTimeMillis();
			if (messages == null || messages.isEmpty())
				return resp.response(EProcessResponse.Failed);

			for (Message<String> m : messages) {
				String line = String.format("[%d] %s\n", count, m.data());
				f_lock.lock();
				try {
					writer.write(line.getBytes("UTF-8"));
				} finally {
					f_lock.unlock();
				}
				count++;
				f_count++;
			}
			writer.flush();

			r_time += System.currentTimeMillis() - ts;

			if (sleept > 0)
				Thread.sleep(sleept);
			bcount += messages.size();

			if (bcount >= 200000) {
				LogUtils.debug(getClass(), String.format(
						"[%s] Processed [%d] records : [AVG:%f][TOT:%d]",
						name(), count, ((double) r_time / count), r_time));
				bcount = 0;
			}
			if (f_count > 200000) {
				f_lock.lock();
				try {
					writer.close();
					String filename = filename();
					writer = new FileOutputStream(filename);
				} finally {
					f_lock.unlock();
				}
				f_count = 0;
			}
			resp.messages(messages).response(EProcessResponse.Success);

		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
			resp.error(t, name()).response(EProcessResponse.Failed).response();
		}
		return resp;
	}

	@Override
	protected ProcessResponse<String> process(Message<String> message)
			throws ProcessingException, NonFatalProcessorException {
		return null;
	}

	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		super.configure(config);
		try {
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigUtils.parse(config, this);

			if (StringUtils.isEmpty(directory))
				throw new ConfigurationException(
						"Missing directory parameter.");
			directory = String.format("%s/%s", directory, subscriberName);
			File f = new File(directory);
			if (!f.exists()) {
				f.mkdirs();
			}

			String filename = filename();
			writer = new FileOutputStream(filename);

		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
			throw new ConfigurationException("Error configuring producer.", t);
		}
	}

	private String filename() {
		return String.format("%s/%s-OUTPUT-%s_%d.out", directory, name(), id(),
				f_index++);
	}

	@Override
	public void dispose() {
		try {
			if (writer != null)
				writer.close();
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
		}
	}

	@Override
	public void run() {
		try {
			MessagePullSubscriber<String> puller = (MessagePullSubscriber<String>) subscriber;
			long lastread = System.currentTimeMillis();
			while (true) {
				if (System.currentTimeMillis()
						- lastread > Constants.READ_TIMEOUT)
					break;

				long ts = System.currentTimeMillis();
				List<Message<String>> messages = puller.batch(1024, 100);

				r_time += System.currentTimeMillis() - ts;

				if (messages != null && !messages.isEmpty()) {
					execute(messages);
					List<String> acks = new ArrayList<String>(messages.size());
					for (Message<String> m : messages) {
						acks.add(m.header().id());
					}
					puller.ack(acks);
					lastread = System.currentTimeMillis();
				}
			}
		} catch (Throwable t) {
			LogUtils.debug(getClass(),
					String.format("[%s] %s", name(), t.getLocalizedMessage()));
			LogUtils.stacktrace(getClass(), t, log);
		}
	}
}
