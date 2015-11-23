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

import com.wookler.server.common.Configurable;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.Publisher;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 20/08/14
 */
@CPath(path = "producer")
public class Producer implements Configurable, Runnable {
	private static final Logger log = LoggerFactory.getLogger(Producer.class);

	public static final class Constants {
		private static final int b_size = 1000;
	}

	@CParam(name = "@name")
	private String				name;
	@CParam(name = "file.input")
	private String				input;
	private Publisher<String>	publisher;
	@CParam(name = "file.cycle", required = false)
	private int					cycle		= 1;
	private int					count		= 0;
	private long				r_time		= 0;
	@CParam(name = "sleep", required = false)
	private int					start_sleep	= 0;

	public Producer(Publisher<String> publisher) {
		this.publisher = publisher;
	}

	public String name() {
		return name;
	}

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the input
	 */
	public String getInput() {
		return input;
	}

	/**
	 * @param input
	 *            the input to set
	 */
	public void setInput(String input) {
		this.input = input;
	}

	/**
	 * @return the cycle
	 */
	public int getCycle() {
		return cycle;
	}

	/**
	 * @param cycle
	 *            the cycle to set
	 */
	public void setCycle(int cycle) {
		this.cycle = cycle;
	}

	/**
	 * @return the start_sleep
	 */
	public int getStart_sleep() {
		return start_sleep;
	}

	/**
	 * @param start_sleep
	 *            the start_sleep to set
	 */
	public void setStart_sleep(int start_sleep) {
		this.start_sleep = start_sleep;
	}

	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		try {
			if (!(config instanceof ConfigPath))
				throw new ConfigurationException(String.format(
						"Invalid config node type. [expected:%s][actual:%s]",
						ConfigPath.class.getCanonicalName(),
						config.getClass().getCanonicalName()));
			ConfigUtils.parse(config, this);
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
			throw new ConfigurationException("Error configuring producer.", t);
		}
	}

	@Override
	public void dispose() {

	}

	@Override
	public void run() {
		try {
			if (start_sleep > 0) {
				Thread.sleep(start_sleep);
			}
			long s_time = System.currentTimeMillis();
			List<String> messages = new ArrayList<String>(Constants.b_size);
			for (int ii = 0; ii < cycle; ii++) {
				BufferedReader br = new BufferedReader(
						new InputStreamReader(new FileInputStream(input)));
				try {
					while (true) {
						String line = br.readLine();
						if (line == null)
							break;
						if (StringUtils.isEmpty(line))
							continue;
						/*
						 * long ts = System.currentTimeMillis();
						 * publisher.publish(line);
						 * r_time += (System.currentTimeMillis() - ts);
						 */

						messages.add(line);
						if (messages.size() == Constants.b_size) {
							long ts = System.currentTimeMillis();
							publisher.publish(messages);
							r_time += (System.currentTimeMillis() - ts);
							messages.clear();
						}

						count++;
					}
				} finally {
					if (br != null)
						br.close();
				}
				LogUtils.debug(getClass(), String.format(
						"[%s] Published [%d] messages : [AVG:%f][TOT:%d]",
						name(), count, ((double) r_time / count), r_time));
			}
			if (messages.size() > 0) {
				long ts = System.currentTimeMillis();
				publisher.publish(messages);
				r_time += (System.currentTimeMillis() - ts);
				messages.clear();
			}
			LogUtils.debug(getClass(),
					String.format(
							"[%s] Published [%d] messages : [AVG:%f][TOT:%d], [ELAPSED:%d]",
							name(), count, ((double) r_time / count), r_time,
							(System.currentTimeMillis() - s_time)));
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
		}
	}
}
