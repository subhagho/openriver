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

import com.wookler.server.common.Reusable;

/**
 * Data structures associated with ACK cache.
 *
 * @author subghosh
 * @created Jun 14, 2015:1:56:37 PM
 *
 */
public class AckCacheStructs {
	/**
	 * Message ACK record used to store ACK status for consumed messages.
	 *
	 * @author subghosh
	 * @created Jun 15, 2015:11:28:17 AM
	 *
	 */
	public static final class MessageAckRecord {
		private String subscriber;
		private String blockId;
		private String messageId;
		private long blockIndex;
		private long sendTimestamp;
		private AckState acked = AckState.FREE;
		
		public MessageAckRecord() {}
		
		public MessageAckRecord(MessageAckRecord rec) {
		    this.subscriber = rec.getSubscriber();
		    this.blockId = rec.getBlockId();
		    this.messageId = rec.getMessageId();
		    this.blockIndex = rec.getBlockIndex();
		    this.sendTimestamp = rec.getSendTimestamp();
		    this.acked = rec.getAcked();
		}
		
		/**
		 * Get the Block ID where the message is stored.
		 *
		 * @return the blockId
		 */
		public String getBlockId() {
			return blockId;
		}

		/**
		 * Set the Block ID where this message is stored.
		 *
		 * @param blockId
		 *            the blockId to set
		 */
		public void setBlockId(String blockId) {
			this.blockId = blockId;
		}

		/**
		 * Get the Message ID for the current message.
		 *
		 * @return the messageId
		 */
		public String getMessageId() {
			return messageId;
		}

		/**
		 * Set the Message ID for the current message.
		 *
		 * @param messageId
		 *            the messageId to set
		 */
		public void setMessageId(String messageId) {
			this.messageId = messageId;
		}

		/**
		 * Get the Block record index for this message.
		 *
		 * @return the blockIndex
		 */
		public long getBlockIndex() {
			return blockIndex;
		}

		/**
		 * Set the Block record index for this message.
		 *
		 * @param blockIndex
		 *            the blockIndex to set
		 */
		public void setBlockIndex(long blockIndex) {
			this.blockIndex = blockIndex;
		}

		/**
		 * Get the timestamp when this message was consumed.
		 *
		 * @return the sendTimestamp
		 */
		public long getSendTimestamp() {
			return sendTimestamp;
		}

		/**
		 * Set the timestamp when this message was consumed.
		 *
		 * @param sendTimestamp
		 *            the sendTimestamp to set
		 */
		public void setSendTimestamp(long sendTimestamp) {
			this.sendTimestamp = sendTimestamp;
		}

		/**
		 * Check if the message has already been acked.
		 *
		 * @return the acked
		 */
		public AckState getAcked() {
			return acked;
		}

		/**
		 * Set the message ACK state.
		 *
		 * @param acked
		 *            the acked to set
		 */
		public void setAcked(AckState acked) {
			this.acked = acked;
		}

		/**
		 * Get the subscriber ID this record belongs to.
		 *
		 * @return the subscriber
		 */
		public String getSubscriber() {
			return subscriber;
		}

		/**
		 * Set the subscriber ID this record belongs to.
		 *
		 * @param subscriber
		 *            the subscriber to set
		 */
		public void setSubscriber(String subscriber) {
			this.subscriber = subscriber;
		}
		
		public void clear() {
		    this.subscriber = null;
		    this.acked = AckState.FREE;
		    this.blockId = null;
		    this.blockIndex = -1;
		    this.messageId = null;
		    this.sendTimestamp = -1;
		}

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "MessageAckRecord [subscriber=" + subscriber + ", blockId=" + blockId + ", messageId=" + messageId + ", blockIndex=" + blockIndex
                    + ", sendTimestamp=" + sendTimestamp + ", acked=" + acked + "]";
        }
		
		
	}

	public enum AckState {
        FREE, ACKED, USED;
    } 
	
	public static final class StructMessageBlockAcks {
		public String blockId;
		public long timestamp;
		public long count = 0;
	}

	public static final class StructSubscriberConfig {
		public String subscriber;
		public int maxSize;
		public int usedSize = 0;
		public long ackTimeout;
	}

	public static final class ReusableAckRecord implements
			Reusable<MessageAckRecord> {

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.wookler.server.common.Reusable#newInstance()
		 */
		@Override
		public MessageAckRecord newInstance() {
			return new MessageAckRecord();
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see com.wookler.server.common.Reusable#dispose(java.lang.Object)
		 */
		@Override
		public void dispose(MessageAckRecord o) {

		}
	}
}
