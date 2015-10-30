/*
 *
 *  Copyright 2014 Subhabrata Ghosh
 *  
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *  
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.wookler.server.river;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.wookler.server.common.ConfigurationException;
import com.wookler.server.common.LockTimeoutException;
import com.wookler.server.common.ReusableObjectFactory;
import com.wookler.server.common.config.ConfigNode;
import com.wookler.server.common.utils.LogUtils;
import com.wookler.server.river.AckCacheStructs.MessageAckRecord;
import com.wookler.server.river.AckCacheStructs.StructMessageBlockAcks;
import com.wookler.server.river.AckCacheStructs.StructSubscriberConfig;

/**
 * TODO: <Write type description>
 *
 * @author subghosh
 * @created Jun 14, 2015:1:55:18 PM
 *
 */
public class BlockingAckCache<M> extends AckCache<M> {

	private HashMap<String, Cache<String, MessageAckRecord>> ackCaches = new HashMap<>();
	private HashMap<String, LinkedList<MessageAckRecord>> resendCaches = new HashMap<>();
	private HashMap<String, StructMessageBlockAcks> blockMap = new HashMap<>();
	private HashMap<String, ReusableObjectFactory<MessageAckRecord>> freeObjects = new HashMap<>();

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.common.Configurable#configure(com.wookler.server.common
	 * .config.ConfigNode)
	 */
	@Override
	public void configure(ConfigNode config) throws ConfigurationException {
		// TODO Auto-generated method stub

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.common.Configurable#dispose()
	 */
	@Override
	public void dispose() {
		if (ackCaches != null && !ackCaches.isEmpty()) {
			for (String k : ackCaches.keySet()) {
				Cache<String, MessageAckRecord> cache = ackCaches.get(k);
				if (cache != null) {
					cache.invalidateAll();
				}
			}
			ackCaches.clear();
		}
	}

	private MessageAckRecord ackLocked(String subscriber, String messageid)
			throws MessageQueueException {
		if (!ackCaches.containsKey(subscriber))
			throw new MessageQueueException(
					"No registered subscriber with ID. [id=" + subscriber + "]");
		Cache<String, MessageAckRecord> cache = ackCaches.get(subscriber);
		MessageAckRecord rec = cache.getIfPresent(messageid);
		if (rec != null) {
		    rec.setAcked(AckCacheStructs.AckState.ACKED);
			cache.invalidate(messageid);
			StructMessageBlockAcks ba = blockMap.get(rec.getBlockId());
			if (ba == null) {
				ba = new StructMessageBlockAcks();
				ba.blockId = rec.getBlockId();
				blockMap.put(ba.blockId, ba);
			}
			ba.count--;
			ba.timestamp = System.currentTimeMillis();
			return rec;
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.river.AckCache#ack(java.lang.String,
	 * java.lang.String)
	 */
	@Override
	public boolean ack(String subscriber, String messageid)
			throws MessageQueueException, LockTimeoutException {
		try {
			if (ackLock.tryLock(AckCache.Constants.LOCK_TIMEOUT,
					TimeUnit.MILLISECONDS)) {
				try {
					MessageAckRecord rec = ackLocked(subscriber, messageid);
					if (rec != null) {
						ReusableObjectFactory<MessageAckRecord> fo = freeObjects
								.get(subscriber);
						if (fo != null) {
							fo.free(rec);
						}
						StructSubscriberConfig c = subscribers.get(subscriber);
						c.usedSize--;
					}
				} finally {
					ackLock.unlock();
				}
			} else
				throw new LockTimeoutException("ACK-CACHE-LOCK",
						"Timeout trying to acquire lock for acking.");
		} catch (LockTimeoutException e) {
			throw e;
		} catch (Exception e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MessageQueueException("Error while acking.", e);
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.river.AckCache#ack(java.lang.String,
	 * java.util.List)
	 */
	@Override
	public void ack(String subscriber, List<String> messageids)
			throws MessageQueueException, LockTimeoutException {
		Preconditions.checkArgument(!StringUtils.isEmpty(subscriber));
		Preconditions
				.checkArgument(messageids != null && !messageids.isEmpty());
		try {
			if (ackLock.tryLock(AckCache.Constants.LOCK_TIMEOUT,
					TimeUnit.MILLISECONDS)) {
				try {
					List<MessageAckRecord> recs = new LinkedList<>();
					for (String mid : messageids) {
						MessageAckRecord rec = ackLocked(subscriber, mid);
						if (rec != null) {
						    rec.clear();
							recs.add(rec);
						}
					}
					if (!recs.isEmpty()) {
						ReusableObjectFactory<MessageAckRecord> fo = freeObjects
								.get(subscriber);
						if (fo != null)
							fo.free(recs);
						StructSubscriberConfig c = subscribers.get(subscriber);
						c.usedSize -= recs.size();
					}
				} finally {
					ackLock.unlock();
				}
			} else
				throw new LockTimeoutException("ACK-CACHE-LOCK",
						"Timeout trying to acquire lock for acking.");
		} catch (LockTimeoutException e) {
			throw e;
		} catch (Exception e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MessageQueueException("Error while acking.", e);
		}
	}

	private void addLocked(String subscriber, MessageAckRecord rec, boolean updateBlockMap)
			throws MessageQueueException {
		Cache<String, MessageAckRecord> cache = ackCaches.get(subscriber);
		if (cache == null)
			throw new MessageQueueException(
					"No subscriber registered with ID. [id=" + subscriber + "]");
		cache.put(rec.getMessageId(), rec);
		// update the blockMap only if the flag is true (only for new messages)
		if (updateBlockMap) {
		    StructMessageBlockAcks ba = blockMap.get(rec.getBlockId());
		    if (ba == null) {
		        ba = new StructMessageBlockAcks();
		        ba.blockId = rec.getBlockId();
		        blockMap.put(ba.blockId, ba);
		    }
		    ba.count++;
		    ba.timestamp = System.currentTimeMillis();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.river.AckCache#add(java.lang.String,
	 * com.wookler.server.river.Message)
	 */
	@Override
	public void add(String subscriber, Message<M> message, MessageAckRecord rec, int resendCount)
			throws MessageQueueException, LockTimeoutException {
		Preconditions.checkArgument(!StringUtils.isEmpty(subscriber));
		Preconditions.checkArgument(message != null);
		Preconditions.checkArgument(rec != null);
		try {
			if (ackLock.tryLock(AckCache.Constants.LOCK_TIMEOUT,
					TimeUnit.MILLISECONDS)) {
				try {
				    rec.setAcked(AckCacheStructs.AckState.USED);
					rec.setBlockId(message.header().blockid());
					rec.setBlockIndex(message.header().blockindex());
					rec.setMessageId(message.header().id());
					rec.setSendTimestamp(message.header().sendtime());
					rec.setSubscriber(subscriber);
					boolean updateBlockMap = true;
					if (resendCount == 1) {
					    updateBlockMap = false;
					}
					addLocked(subscriber, rec, updateBlockMap);
				} finally {
					ackLock.unlock();
				}
			} else
				throw new LockTimeoutException("ACK-CACHE-LOCK",
						"Timeout trying to acquire lock for acking.");
		} catch (LockTimeoutException e) {
			throw e;
		} catch (Exception e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MessageQueueException("Error while acking.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.river.AckCache#add(java.lang.String,
	 * java.util.List)
	 */
	@Override
	public void add(String subscriber, List<Message<M>> messages,
			List<MessageAckRecord> recs, int resendCount) throws MessageQueueException,
			LockTimeoutException {
		Preconditions.checkArgument(!StringUtils.isEmpty(subscriber));
		Preconditions.checkArgument(messages != null && !messages.isEmpty());
		Preconditions.checkArgument(recs != null);
		Preconditions.checkArgument(recs.size() >= messages.size());

		try {
			if (ackLock.tryLock(AckCache.Constants.LOCK_TIMEOUT,
					TimeUnit.MILLISECONDS)) {
				try {
					int ii = -1;
					int r = resendCount;
					// In the messages list, the first r messages correspond to the resend messages, remaining (size - r) messages correspond to new messages
					for (ii = 0; ii < messages.size(); ii++) {
						Message<M> message = messages.get(ii);
						MessageAckRecord rec = recs.get(ii);
						if ( rec.getAcked() != AckCacheStructs.AckState.FREE) {
						    throw new MessageQueueException("Ack state is not free. Record: " + rec.toString());
						}
						rec.setAcked(AckCacheStructs.AckState.USED);
						rec.setBlockId(message.header().blockid());
						rec.setBlockIndex(message.header().blockindex());
						rec.setMessageId(message.header().id());
						rec.setSendTimestamp(message.header().sendtime());
						rec.setSubscriber(subscriber);
						// blockMap should be updated with the count only for new messages that are received. If the messages are being
						// resent, the the blockMap already holds the count.
						boolean updateBlockMap = true;
						if (r > 0) { 
						    updateBlockMap = false;
						}
						addLocked(subscriber, rec, updateBlockMap);
						// decrement the resend count after each message is added
						r--;
					}
					if (ii < recs.size()) {
						ReusableObjectFactory<MessageAckRecord> fo = freeObjects
								.get(subscriber);
						for (int jj = ii; jj < recs.size(); jj++) {
							if (fo != null) {
								fo.free(recs.get(jj));
							}
						}
					}
				} finally {
					ackLock.unlock();
				}
			} else
				throw new LockTimeoutException("ACK-CACHE-LOCK",
						"Timeout trying to acquire lock for acking.");
		} catch (LockTimeoutException e) {
			throw e;
		} catch (Exception e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MessageQueueException("Error while acking.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.river.AckCache#hasMessagesForResend(java.lang.String)
	 */
	@Override
	public boolean hasMessagesForResend(String subscriber)
			throws MessageQueueException {
		if (resendCaches.containsKey(subscriber)) {
			LinkedList<MessageAckRecord> cache = resendCaches.get(subscriber);
			if (!cache.isEmpty()) {
				return true;
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.river.AckCache#getMessagesForResend(java.lang.String)
	 */
	@Override
	public List<Message<M>> getMessagesForResend(String subscriber,
			int batchSize) throws MessageQueueException, LockTimeoutException {
		try {
			if (resendCaches.containsKey(subscriber)) {
				List<MessageAckRecord> records = null;
				LinkedList<MessageAckRecord> cache = resendCaches
						.get(subscriber);
				if (!cache.isEmpty()) {
					if (resendLock.tryLock(AckCache.Constants.LOCK_TIMEOUT,
							TimeUnit.MILLISECONDS)) {
						try {
							int s = batchSize;
							if (s > cache.size()) {
								s = cache.size();
							}
							if (s > 0) {
								records = new LinkedList<>();
								for (int ii = 0; ii < s; ii++) {
								    MessageAckRecord r = cache.pop();
									records.add(r);
								}
							}

							if (records != null && !records.isEmpty()) {
								HashMap<String, LinkedList<MessageAckRecord>> rMap = new HashMap<>();
								for (MessageAckRecord rec : records) {
									LinkedList<MessageAckRecord> l = null;
									if (rMap.containsKey(rec.getBlockId())) {
										l = rMap.get(rec.getBlockId());
									} else {
										l = new LinkedList<>();
										rMap.put(rec.getBlockId(), l);
									}
									l.add(rec);
								}
								List<Message<M>> messages = new LinkedList<>();
								for (String b : rMap.keySet()) {
									LinkedList<MessageAckRecord> l = rMap
											.get(b);
									List<Message<M>> ms = queue.readForResend(l
											.get(0).getBlockId(), l);
									if (!ms.isEmpty()) {
										messages.addAll(ms);
									}
								}
								if (!messages.isEmpty()) {
									return messages;
								}
							}
						} finally {
							resendLock.unlock();
						}
					}
				}
			}
			return null;
		} catch (Exception e) {
			LogUtils.stacktrace(getClass(), e);
			throw new MessageQueueException(
					"Error getting messages for resend.", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.river.AckCache#hasPendingAcks(java.lang.String)
	 */
	@Override
	public boolean hasPendingAcks(String blockid) {
		if (blockMap.containsKey(blockid)) {
			StructMessageBlockAcks b = blockMap.get(blockid);
			if (b != null) {
				return b.count > 0;
			}
		}
		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.river.AckCache#addSubscriber(com.wookler.server.river
	 * .Subscriber)
	 */
	@Override
	public AckCache<M> addSubscriber(Subscriber<M> subscriber) {
		super.addSubscriber(subscriber);
		StructSubscriberConfig c = subscribers.get(subscriber.name);

		RemovalListener<String, MessageAckRecord> evictionListener = new RemovalListener<String, MessageAckRecord>() {
			public void onRemoval(
					RemovalNotification<String, MessageAckRecord> kv) {
                /**
                 * IMPORTANT : The record that is being evicted from the
                 * ackCache indicates that the timeout happened before the ack
                 * was received. So in order for the messages to be resent at a
                 * later point of time, we need to add the MessageAckRecord to
                 * the resendCache. A deep copy of the MessageAckRecord is made,
                 * since the MessageAckRecord that is obtained from the ackCache
                 * is part of ReusableObjectFactory. So once the record is
                 * evicted from the ackCache, it is returned back to the pool to
                 * be used later. So a deep copy is made not to lose the info
                 * pertaining to unacked messages. While the message is being
                 * evicted from the ackCache, the blockMap is not decremented
                 * for count. This is done because the blockMap count is used to
                 * decide if a block is a candidate for gc. The block which has
                 * pending unacked messages should never be GCed. At the time of
                 * add(), the block count is not incremented for messages that
                 * are being resent (if we do not take care of this, it will
                 * result in double counting, and the blocks will never be GCed.
                 * On the other hand, if we decrement the count here, then we
                 * would GC a block prematurely).
                 **/
				if (kv.wasEvicted() && kv.getValue().getAcked() == AckCacheStructs.AckState.USED) {
					LinkedList<MessageAckRecord> records = resendCaches.get(kv
							.getValue().getSubscriber());
					if (records != null) {
						try {
							if (resendLock.tryLock(
									AckCache.Constants.LOCK_TIMEOUT,
									TimeUnit.MILLISECONDS)) {
								try {
								    // deep copy
									records.add(new MessageAckRecord(kv.getValue()));
								} finally {
									resendLock.unlock();
								}
							} else {
								LogUtils.error(getClass(),
										"Timeout getting resend cache lock.");
							}
						} catch (Exception e) {
							LogUtils.stacktrace(getClass(), e);
							throw new RuntimeException(
									"Error handling cache eviction", e);
						}
					}
					// return the MessageAckRecord back to the pool for reuse
					ReusableObjectFactory<MessageAckRecord> fo = freeObjects.get(kv
	                        .getValue().getSubscriber());
	                if (fo != null) {
	                    MessageAckRecord rec = kv.getValue();
	                    rec.clear();
	                    fo.free(rec);
	                }
				}
			}
		};

		Cache<String, MessageAckRecord> cache = CacheBuilder.newBuilder()
				.maximumSize(c.maxSize)
				.expireAfterWrite(c.ackTimeout, TimeUnit.MILLISECONDS)
				.removalListener(evictionListener).build();

		ackCaches.put(subscriber.name(), cache);
		LinkedList<MessageAckRecord> resend = new LinkedList<>();
		resendCaches.put(subscriber.name, resend);

		ReusableObjectFactory<MessageAckRecord> factory = new ReusableObjectFactory<MessageAckRecord>(
				(int) c.maxSize, new AckCacheStructs.ReusableAckRecord());
		freeObjects.put(subscriber.name, factory);

		return this;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.wookler.server.river.AckCache#canAddMessages(java.lang.String,
	 * long)
	 */
	@Override
	public List<MessageAckRecord> allocateAckCache(String subscriber, int count) {
		if (ackCaches.containsKey(subscriber)) {
			StructSubscriberConfig c = subscribers.get(subscriber);
			int rem = c.maxSize - c.usedSize;
			if (rem > count)
				rem = count;
			c.usedSize += rem;
			ReusableObjectFactory<MessageAckRecord> fo = freeObjects
					.get(subscriber);
			return fo.get(rem);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.wookler.server.river.AckCache#canAllocateAckCache(java.lang.String,
	 * int)
	 */
	@Override
	public int canAllocateAckCache(String subscriber, int count) {
		if (ackCaches.containsKey(subscriber)) {
			StructSubscriberConfig c = subscribers.get(subscriber);
			int rem = c.maxSize - c.usedSize;
			if (rem > count)
				rem = count;
			return rem;
		}
		return 0;
	}

}
