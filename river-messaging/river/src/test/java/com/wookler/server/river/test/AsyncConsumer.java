package com.wookler.server.river.test;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.config.ConfigAttributes;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.config.ConfigParams;
import com.wookler.server.common.config.ConfigPath;
import com.wookler.server.common.config.ConfigUtils;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.EProcessResponse;
import com.wookler.server.river.Message;
import com.wookler.server.river.MessageQueueException;
import com.wookler.server.river.NonFatalProcessorException;
import com.wookler.server.river.ProcessResponse;
import com.wookler.server.river.ProcessingException;
import com.wookler.server.river.SubscriberAwareProcessor;

/**
 * 
 * Subscriber aware Async Consumer that is responsible for invoking explicit ack
 * on the processed messages
 *
 */
public class AsyncConsumer extends SubscriberAwareProcessor<String> {
    private static final Logger log = LoggerFactory.getLogger(AsyncConsumer.class);

    public static final class Constants {
        public static final String CONFIG_NODE_NAME = "consumer";

        public static final String CONFIG_ATTR_NAME = "name";
        public static final String CONFIG_OUTPUT_DIR = "directory.output";
        public static final String CONFIG_SLEEP = "processor.sleep";
        public static final String CONFIG_QUEUE_NAME = "queue.pull.name";
    }

    private String directory;
    private FileOutputStream writer;
    private int count = 0;
    private long sleept = -1;
    private int bcount = 0;

    private long r_time = 0;
    private int f_index = 0;
    private int f_count = 0;
    private ReentrantLock f_lock = new ReentrantLock();
    LinkedBlockingQueue<List<String>> ackListQueue = new LinkedBlockingQueue<List<String>>();

    @Override
    protected ProcessResponse<String> process(List<Message<String>> messages) throws ProcessingException, NonFatalProcessorException {
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
                LogUtils.debug(getClass(),
                        String.format("[%s] Processed [%d] records : [AVG:%f][TOT:%d]", name(), count, ((double) r_time / count), r_time));
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

            // add the message id batch to be acked to the ack queue
            List<String> acks = new ArrayList<String>();
            for (Message<String> m : messages) {
                acks.add(m.header().id());
            }
            ackListQueue.add(acks);
        } catch (Throwable t) {
            LogUtils.stacktrace(getClass(), t, log);
            resp.response(EProcessResponse.Failed).response().error(t);
        }
        return resp;
    }

    @Override
    protected ProcessResponse<String> process(Message<String> message) throws ProcessingException, NonFatalProcessorException {
        return null;
    }

    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        super.configure(config);
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format("Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));
            ConfigAttributes attr = ConfigUtils.attributes(config);
            name(attr.attribute(Constants.CONFIG_ATTR_NAME));
            if (StringUtils.isEmpty(name()))
                throw new ConfigurationException("Missing attribute. [attribute=" + Constants.CONFIG_ATTR_NAME + "]");
            log.debug(String.format("Configuring producer. [name=%s]", name()));

            ConfigParams params = ConfigUtils.params(config);

            String s = params.param(Constants.CONFIG_SLEEP);
            if (!StringUtils.isEmpty(s))
                sleept = Long.parseLong(s);

            directory = params.param(Constants.CONFIG_OUTPUT_DIR);
            if (StringUtils.isEmpty(directory))
                throw new ConfigurationException("Missing parameter. [name=" + Constants.CONFIG_OUTPUT_DIR + "]");
            directory = String.format("%s/%s", directory, this.subscriber.name());
            File f = new File(directory);
            if (!f.exists()) {
                f.mkdirs();
            }

            String filename = filename();
            writer = new FileOutputStream(filename);

            // start a Acker thread
            if (this.getSubscriber().subscriberAsyncAck()) {
                LogUtils.mesg(getClass(), "Starting acker thread...");
                AckerThread ackerTh = new AckerThread(this);
                Thread th = new Thread(ackerTh);
                th.start();
            }

        } catch (Throwable t) {
            LogUtils.stacktrace(getClass(), t, log);
            throw new ConfigurationException("Error configuring producer.", t);
        }
    }

    private String filename() {
        return String.format("%s/%s-OUTPUT-%s_%d.out", directory, name(), id(), f_index++);
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

    protected static final class AckerThread implements Runnable {

        private AsyncConsumer processor;

        public AckerThread(AsyncConsumer processor) {
            this.processor = processor;
        }

        @Override
        public void run() {
            while (true) {
                // poll the queue and invoke ack
                List<String> acks = processor.ackListQueue.poll();
                try {
                    if (acks != null) {
                        processor.getSubscriber().ack(acks);
                    }
                    /**
                     * IMPORTANT: Commenting out the sleep part for now. If we
                     * delay the ack by sleeping, then we trigger lot of resends
                     * and mess up the gc. This can't be fixed currently, unless
                     * we change the caching mechanism used for acking
                     **/
                    Thread.sleep(100);
                } catch (MessageQueueException e) {
                    LogUtils.stacktrace(getClass(), e, log);
                } catch (InterruptedException e) {
                    LogUtils.stacktrace(getClass(), e, log);
                }
            }
        }

    }
}
