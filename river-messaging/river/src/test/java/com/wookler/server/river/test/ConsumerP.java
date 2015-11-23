/*
 * Copyright [2014] Subhabrata Ghosh
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.river.test;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 20/08/14
 */
public class ConsumerP extends Processor<String> {
	private static final Logger log = LoggerFactory.getLogger(ConsumerP.class);

	private int					count	= 0;
	@CParam(name = "processor.sleep", required = false)
	private long				sleept	= -1;
	private Publisher<String>	publisher;
	private int					bcount	= 0;
	@CParam(name = "queue.name.output")
	private String				qname;
	private long				r_time	= 0;

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
	 * @return the qname
	 */
	public String getQname() {
		return qname;
	}

	/**
	 * @param qname
	 *            the qname to set
	 */
	public void setQname(String qname) {
		this.qname = qname;
	}

	@Override
	protected ProcessResponse<String> process(List<Message<String>> messages)
			throws ProcessingException, NonFatalProcessorException {
		ProcessResponse<String> resp = new ProcessResponse<String>();
		try {
			long ts = System.currentTimeMillis();
			if (messages == null || messages.isEmpty())
				resp.response(EProcessResponse.Failed);

			List<String> data = new ArrayList<String>();
			for (Message<String> m : messages) {
				data.add(m.data());
			}
			publisher.publish(data);

			r_time += System.currentTimeMillis() - ts;

			if (sleept > 0)
				Thread.sleep(sleept);
			bcount += messages.size();
			count += messages.size();

			if (bcount >= 200000) {
				LogUtils.debug(getClass(), String.format(
						"[%s] Processed [%d] records : [AVG:%f][TOT:%d]",
						name(), count, ((double) r_time / count), r_time));
				bcount = 0;
			}

			resp.messages(messages).response(EProcessResponse.Success);
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
			resp.error(t, name()).response(EProcessResponse.Exception)
					.response();
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

			log.debug(String.format("Using output Queue. [name=%s]", qname));

			MessageQueue<String> queue = Test_MessageExecutorMultiQ
					.queue(qname);
			if (queue == null)
				throw new ConfigurationException(
						"Invalid Queue name specified. [queue=" + qname + "]");
			publisher = queue.publisher();
		} catch (Throwable t) {
			LogUtils.stacktrace(getClass(), t, log);
			throw new ConfigurationException("Error configuring producer.", t);
		}
	}

	@Override
	public void dispose() {

	}
}
