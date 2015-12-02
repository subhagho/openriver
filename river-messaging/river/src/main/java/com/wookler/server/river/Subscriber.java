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

package com.wookler.server.river;

import com.wookler.server.common.*;
import com.wookler.server.common.config.*;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.common.utils.Monitoring;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Base class for defining subscribers to the queue.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 12/08/14
 */
@CPath(path = "subscriber")
public abstract class Subscriber<M> implements Configurable, AckHandler {
    private static final Logger log = LoggerFactory.getLogger(Subscriber.class);

    public static final class Constants {
        public static final String MONITOR_NAMESPACE = "river.counters.subscriber";
        public static final String MONITOR_COUNTER_ACKS = "acks";
        public static final String MONITOR_COUNTER_RESEND = "resend";
        public static final String MONITOR_COUNTER_READS = "reads";
        public static final String MONITOR_COUNTER_READTIME = "time.read";

        public static final int RETRY_COUNT = 3;
    }

    /** counters at this subscriber level */
    private HashMap<String, String[]> counters = new HashMap<String, String[]>();

    /**
     * Instance setState.
     */
    protected ObjectState state = new ObjectState();
    /**
     * Instance name.
     */
    @CParam(name = "@name")
    protected String name;
    /**
     * Configured batch size for de-queuing messages.
     */
    @CParam(name = "subscriber.batch.size")
    protected int batchSize;
    /**
     * Configured timeout for Queue operations.
     */
    @CParam(name = "subscriber.poll.timeout")
    protected long queueTimeout;
    /**
     * Message delivery expects acknowledgement?
     */
    @CParam(name = "subscriber.ack.required", required = false)
    protected boolean ackrequired = false;
    /**
     * Whether the messages should be acked in an async manner?
     */
    @CParam(name = "subscriber.ack.async", required = false)
    protected boolean subscriberAckAsync = false;
    /**
     * Configured size of the resend cache.
     */
    @CParam(name = "subscriber.ack.cache.size", required = false)
    protected int cachesize = -1;
    /**
     * Message fetch lock.
     */
    protected ReentrantLock lock = new ReentrantLock();

    /** subscriber operation retry count */
    @CParam(name = "subscriber.retry.count", required = false)
    protected int retryCount = Constants.RETRY_COUNT;

    /** subscriber ack timeout */
    @CParam(name = "subscriber.ack.timeout", required = false)
    private long acktimeout;
    /** message queue to which this subscriber is subscribed to */
    private Queue<M> queue;
    /** ack cache instance */
    private AckCache<M> ackCache;

    /**
     * Get the subscriber setState.
     *
     * @return - Subscriber setState.
     */
    public ObjectState state() {
        return state;
    }

    /**
     * Set the queue for this subscriber.
     *
     * @param queue
     *            - Parent Queue.
     * @return - self
     */
    public Subscriber<M> queue(Queue<M> queue) {
        this.queue = queue;
        return this;
    }

    /**
     * Get the queue for this subscriber.
     *
     * @return - Message Queue
     */
    public Queue<M> queue() {
        return queue;
    }

    /**
     * Set the ACK cache handle for this subscriber;
     *
     * @param ackCache
     *            ackCahce instance
     * @return
     */
    public Subscriber<M> ackCache(AckCache<M> ackCache) {
        this.ackCache = ackCache;
        this.ackCache.addSubscriber(this);
        return this;
    }

    /**
     * Get the ACK cache handle for this subscriber.
     *
     * @return - ACK Cache handle.
     */
    public AckCache<M> ackCache() {
        return ackCache;
    }

    /**
     * Set the read batch size.
     *
     * @param batchSize
     *            - Number of messages to be read per call.
     * @return - self.
     */
    public Subscriber<M> batchSize(int batchSize) {
        this.batchSize = batchSize;

        return this;
    }

    /**
     * Get the read batch size.
     *
     * @return - Configured read batch size.
     */
    public int batchSize() {
        return batchSize;
    }

    /**
     * Set the queue read polling timeout.
     *
     * @param batchTimeout
     *            - Timeout after which the read call will return.
     * @return - self.
     */
    public Subscriber<M> queueTimeout(long batchTimeout) {
        this.queueTimeout = batchTimeout;

        return this;
    }

    /**
     * Get the queue operation timeout.
     *
     * @return - Configured Queue operation timeout.
     */
    public long queueTimeout() {
        return queueTimeout;
    }

    /**
     * Get the name of this subscriber.
     *
     * @return - Subsciber name.
     */
    public String name() {
        return name;
    }

    /**
     * Start the message subscriber.
     *
     * @throws MessageQueueException
     */
    public void start() throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Initialized, getClass());
            state.setState(EObjectState.Available);
        } catch (StateException e) {
            throw new MessageQueueException("Message subscriber in invalid setState.", e);
        }
    }

    /**
     * Does this subscriber need acks?
     *
     * @return - ACK required?
     */
    public boolean ackrequired() {
        return ackrequired;
    }

    /**
     * Get the configured ACK timeout for this subscriber.
     *
     * @return acktimeout
     */
    public long acktimeout() {
        return acktimeout;
    }

    /**
     * Are the subscriber acks async?
     *
     * @return - async ack?
     */
    public boolean subscriberAsyncAck() {
        return subscriberAckAsync;
    }

    /**
     * Get the ACK cache size for this subscriber.
     *
     * @return - Max Cache size, -1 if ackrequired = false
     */
    public int cacheSize() {
        if (ackrequired)
            return cachesize;
        else
            return -1;
    }

    /**
     * Base configuration for a subscriber. Sample:
     * <p/>
     * 
     * <pre>
     * {@code
     *      <subscriber class="com.wookler.server.river.MessageProcessor" name="[NAME]">
     *          <params>
     *              <param name="subscriber.batch.size" value="[batch size]" />
     *              <param name="subscriber.poll.timeout" value="[queue poll timeout]" />
     *              <param name="sleep.interval" value="[optional]" />
     *              <param name="subscriber.ack.required" value="[optional: default=false]" />
     *              <param name="subscriber.ack.async" value="[optional : default=false]" />
     *              <param name="subscriber.ack.cache.size" value="[required: if subscriber.ack.required=true]" />
     *              <param name="subscriber.ack.timeout" value="[required: if subscriber.ack.required=true]" />
     *          </params>
     *       </subscriber>
     * }
     * </pre>
     *
     * @param config
     *            - Configuration node for this instance.
     * @throws ConfigurationException
     */
    @Override
    public void configure(ConfigNode config) throws ConfigurationException {
        try {
            if (!(config instanceof ConfigPath))
                throw new ConfigurationException(String.format(
                        "Invalid config node type. [expected:%s][actual:%s]",
                        ConfigPath.class.getCanonicalName(), config.getClass().getCanonicalName()));

            ConfigUtils.parse(config, this);

            if (ackrequired) {
                if (cachesize <= 0)
                    throw new ConfigurationException("ACK cache size not specified.");
                if (acktimeout <= 0)
                    throw new ConfigurationException("ACK timeout not specified.");
            }

            registerCounters();
        } catch (ConfigurationException ce) {
            exception(ce);
            throw ce;
        }
    }

    /**
     * Ack message for this subscriber.
     *
     * @param messageid
     *            - Message ID.
     * @throws MessageQueueException
     */
    @Override
    public void ack(String messageid) throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Available, Subscriber.class);
            if (ackrequired) {
                lock.lock();
                try {
                    ackCache.ack(name, messageid);
                    incrementCounter(Constants.MONITOR_COUNTER_ACKS, 1);
                } finally {
                    lock.unlock();
                }
            }
        } catch (StateException oe) {
            throw new MessageQueueException("Error performing ACK operation.", oe);
        } catch (LockTimeoutException e) {
            throw new MessageQueueException("Timeout while performing ACK operation.", e);
        }
    }

    /**
     * Ack message batch for this subscriber.
     *
     * @param messageids
     *            - List of message IDs.
     * @throws MessageQueueException
     */
    @Override
    public void ack(List<String> messageids) throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Available, Subscriber.class);
            if (ackrequired) {
                lock.lock();
                try {
                    ackCache.ack(name, messageids);
                    incrementCounter(Constants.MONITOR_COUNTER_ACKS, messageids.size());
                } finally {
                    lock.unlock();
                }
            }
        } catch (StateException oe) {
            throw new MessageQueueException("Error performing ACK operation.", oe);
        } catch (LockTimeoutException e) {
            throw new MessageQueueException("Timeout while performing ACK operation.", e);
        }
    }

    /**
     * Perform cleanup tasks if any required. Called by the queue management
     * thread.
     *
     * @throws MessageQueueException
     */
    public void cleanup() throws MessageQueueException {

    }

    /**
     * Get the next batch of messages. If ackrequired is set, then a check is
     * made to see if ackCache can accommodate new messages. If it can, then
     * first ack cache is checked for any messages to be resent. Then if more
     * messages can be accommodated in the ackCache, then new messages are read
     * from the queue. If ackCacke is already full, then only messages that need
     * to resent are checked and tried again. If ackrequired is not set, then
     * new messages are read from the queue.
     *
     * @param size
     *            - Batch size.
     * @param timeout
     *            - Fetch timeout.
     * @return - List of messages.
     * @throws MessageQueueException
     */
    protected List<Message<M>> batch(int size, long timeout) throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Available, Subscriber.class);
            if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                try {
                    List<Message<M>> read = new LinkedList<>();
                    // if ack is configured
                    if (ackrequired) {
                        int csize = ackCache.canAllocateAckCache(name, size);
                        // if more messages can be accommodated in ackCache
                        if (csize > 0) {
                            int resendCount = 0;

                            List<MessageAckRecord> recs = ackCache.allocateAckCache(name, csize);
                            if (recs != null && !recs.isEmpty()) {
                                csize = recs.size();
                                List<Message<M>> messages = ackCache.getMessagesForResend(name,
                                        csize);
                                if (messages != null && !messages.isEmpty()) {
                                    read.addAll(messages);
                                    incrementCounter(Constants.MONITOR_COUNTER_RESEND,
                                            messages.size());
                                }
                                // set the resendCount to the number of messages
                                // that require to be resent.
                                resendCount = read.size();
                                int rem = csize - read.size();
                                // if more messages can be accommodated in
                                // cache, then read new messages from queue
                                readFromQueue(rem, read, timeout);
                            }
                            if (!read.isEmpty()) {
                                // invoke ackCache.add() along with the
                                // resendCount
                                ackCache.add(name, read, recs, resendCount);
                            }
                        } else {
                            // no more cache can be allocated. So just check for
                            // resend messages.
                            List<Message<M>> messages = ackCache.getMessagesForResend(name, size);
                            if (messages != null && !messages.isEmpty()) {
                                read.addAll(messages);
                                incrementCounter(Constants.MONITOR_COUNTER_RESEND, messages.size());
                                List<MessageAckRecord> recs = ackCache.allocateAckCache(name,
                                        messages.size());
                                ackCache.add(name, read, recs, read.size());
                            }
                        }
                        // ack required is false, just read message batch from
                        // the queue
                    } else {
                        readFromQueue(size, read, timeout);
                    }
                    return read;
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            throw new MessageQueueException(String.format("[%s:%s] Lock interrupted during next.",
                    getClass(), name()));
        } catch (LockTimeoutException te) {
            LogUtils.stacktrace(getClass(), te);
            LogUtils.mesg(getClass(), te.getLocalizedMessage());
        } catch (StateException e) {
            throw new MessageQueueException(
                    "Error in getting message batch. Not able to check the subscriber state", e);
        }
        return null;
    }

    /**
     * Read message batch from the queue
     *
     * @param size
     *            the batch size
     * @param read
     *            the list containing the messages that are read from the queue
     * @param timeout
     *            the fetch timeout
     * @throws MessageQueueException
     *             the message queue exception
     * @throws LockTimeoutException
     *             the lock timeout exception
     */
    private void readFromQueue(int size, List<Message<M>> read, long timeout)
            throws MessageQueueException, LockTimeoutException {
        if (size > 0) {
            long startt = System.currentTimeMillis();
            int count = 0;
            try {
                List<Message<M>> messages = queue.batch(name, size, timeout);
                if (messages != null && !messages.isEmpty()) {
                    count = messages.size();
                    incrementCounter(Constants.MONITOR_COUNTER_READS, count);
                    read.addAll(messages);
                }
            } finally {
                if (count > 0)
                    timerstop(Constants.MONITOR_COUNTER_READTIME, startt, count);
            }
        }
    }

    /**
     * Get the next message in the queue.
     *
     * @param timeout
     *            - Fetch timeout.
     * @return - Next message.
     * @throws MessageQueueException
     */
    protected Message<M> next(long timeout) throws MessageQueueException {
        try {
            ObjectState.check(state, EObjectState.Available, Subscriber.class);
            if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                try {
                    int csize = ackCache.canAllocateAckCache(name, 1);
                    if (csize > 0) {
                        List<MessageAckRecord> recs = ackCache.allocateAckCache(name, csize);
                        if (recs != null && !recs.isEmpty()) {
                            if (ackCache.hasMessagesForResend(name)) {
                                List<Message<M>> messages = ackCache.getMessagesForResend(name,
                                        csize);
                                if (messages != null && !messages.isEmpty()) {
                                    // found a message that requires resend
                                    // (resendCount = 1)
                                    ackCache.add(name, messages, recs, 1);
                                    incrementCounter(Constants.MONITOR_COUNTER_RESEND, 1);
                                    return messages.get(0);
                                }
                            }

                            long startt = System.currentTimeMillis();
                            int count = 0;
                            try {
                                Message<M> message = queue.poll(name, timeout);
                                if (message != null) {
                                    count = 1;
                                    incrementCounter(Constants.MONITOR_COUNTER_READS, count);
                                    // new message (resendCount = 0)
                                    ackCache.add(name, message, recs.get(0), 0);
                                    return message;
                                }
                            } finally {
                                if (count > 0)
                                    timerstop(Constants.MONITOR_COUNTER_READTIME, startt, 1);
                            }
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        } catch (InterruptedException e) {
            throw new MessageQueueException(String.format("[%s:%s] Lock interrupted during next.",
                    getClass(), name()));
        } catch (LockTimeoutException te) {
            LogUtils.mesg(getClass(), te.getLocalizedMessage());
        } catch (StateException e) {
            throw new MessageQueueException(String.format("[%s:%s] Invalid Subscriber state. [%s]",
                    getClass(), name(), e.getLocalizedMessage()), e);
        }
        return null;
    }

    /**
     * Set the setState to getError.
     *
     * @param t
     *            - Exception.
     */
    protected void exception(Throwable t) {
        state.setState(EObjectState.Exception).setError(t);
        LogUtils.stacktrace(getClass(), t, log);
    }

    /**
     * Register counters for this subscriber (ack count, resend count, read
     * count, read time)
     */
    protected void registerCounters() {

        AbstractCounter c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
                Constants.MONITOR_COUNTER_ACKS, Count.class, AbstractCounter.Mode.PROD);
        if (c != null) {
            counters.put(Constants.MONITOR_COUNTER_ACKS, new String[] { c.namespace(), c.name() });
        }
        c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
                Constants.MONITOR_COUNTER_RESEND, Count.class, AbstractCounter.Mode.PROD);
        if (c != null) {
            counters.put(Constants.MONITOR_COUNTER_RESEND, new String[] { c.namespace(), c.name() });
        }
        c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
                Constants.MONITOR_COUNTER_READS, Count.class, AbstractCounter.Mode.PROD);
        if (c != null) {
            counters.put(Constants.MONITOR_COUNTER_READS, new String[] { c.namespace(), c.name() });
        }
        c = Monitoring.create(Constants.MONITOR_NAMESPACE + "." + name,
                Constants.MONITOR_COUNTER_READTIME, Average.class, AbstractCounter.Mode.PROD);
        if (c != null) {
            counters.put(Constants.MONITOR_COUNTER_READTIME,
                    new String[] { c.namespace(), c.name() });
        }
    }

    /**
     * Increment the specified counter.
     *
     * @param name
     *            - Counter key.
     * @param value
     *            - Increment value.
     */
    private void incrementCounter(String name, long value) {
        if (counters.containsKey(name)) {
            String[] names = counters.get(name);
            Monitoring.increment(names[0], names[1], value);
        }
    }

    /**
     * Stop the current timer and update the corresponding counter.
     *
     * @param name
     *            - Counter Key
     * @param starttime
     *            - Start time for this timer.
     * @param count
     *            - Operation count.
     */
    private void timerstop(String name, long starttime, long count) {
        if (counters.containsKey(name)) {
            String[] names = counters.get(name);
            Monitoring.timerstop(starttime, count, names[0], names[1]);
        }
    }

    /**
     * Get the subscriber name
     * 
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * Set the subscriber name
     * 
     * @param name
     *            the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Get the message batch size
     * 
     * @return the batchSize
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Set the message batch size
     * 
     * @param batchSize
     *            the batchSize to set
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Get the queue fetch timeout
     * 
     * @return the queueTimeout
     */
    public long getQueueTimeout() {
        return queueTimeout;
    }

    /**
     * Set the queue fetch timeout
     * 
     * @param queueTimeout
     *            the queueTimeout to set
     */
    public void setQueueTimeout(long queueTimeout) {
        this.queueTimeout = queueTimeout;
    }

    /**
     * Check if ackrequired is set
     * 
     * @return ackrequired?
     */
    public boolean isAckrequired() {
        return ackrequired;
    }

    /**
     * Set the ackrequired flag
     * 
     * @param ackrequired
     *            the ackrequired to set
     */
    public void setAckrequired(boolean ackrequired) {
        this.ackrequired = ackrequired;
    }

    /**
     * Is the ack async ?
     * 
     * @return is subscriberAckAsync?
     */
    public boolean isSubscriberAckAsync() {
        return subscriberAckAsync;
    }

    /**
     * Set the async flag for acks
     * 
     * @param subscriberAckAsync
     *            the subscriberAckAsync to set
     */
    public void setSubscriberAckAsync(boolean subscriberAckAsync) {
        this.subscriberAckAsync = subscriberAckAsync;
    }

    /**
     * Get the ack cache size
     * 
     * @return the ack cache size
     */
    public int getCachesize() {
        return cachesize;
    }

    /**
     * Set the ack cache size
     * 
     * @param cachesize
     *            the cachesize to set
     */
    public void setCachesize(int cachesize) {
        this.cachesize = cachesize;
    }

    /**
     * Get the retry count
     * 
     * @return the retryCount
     */
    public int getRetryCount() {
        return retryCount;
    }

    /**
     * Set the retry count
     * 
     * @param retryCount
     *            the retryCount to set
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    /**
     * Get the ack timeout
     * 
     * @return the acktimeout
     */
    public long getAcktimeout() {
        return acktimeout;
    }

    /**
     * Set the ack timeout
     * 
     * @param acktimeout
     *            the acktimeout to set
     */
    public void setAcktimeout(long acktimeout) {
        this.acktimeout = acktimeout;
    }
}
