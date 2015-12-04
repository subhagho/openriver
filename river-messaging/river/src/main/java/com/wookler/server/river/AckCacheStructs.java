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
        /** subscriber name */
        private String subscriber;
        /** block id to which the message belongs to */
        private String blockId;
        /** message id */
        private String messageId;
        /** message record index within the block */
        private long blockIndex;
        /** message consume timestamp */
        private long sendTimestamp;
        /** Message ack status */
        private AckState acked = AckState.FREE;

        /**
         * Default constructor
         */
        public MessageAckRecord() {
        }

        /**
         * Copy constructor -- Instantiates a new message ack record from the
         * specified record
         *
         * @param rec
         *            the rec
         */
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
         * Get the {@link AckState} of the message
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

        /**
         * Clear the Message ACK record, so that it can be reused.
         */
        public void clear() {
            this.subscriber = null;
            this.acked = AckState.FREE;
            this.blockId = null;
            this.blockIndex = -1;
            this.messageId = null;
            this.sendTimestamp = -1;
        }

        /**
         * Message ack record string representation
         * 
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "MessageAckRecord [subscriber=" + subscriber + ", blockId=" + blockId
                    + ", messageId=" + messageId + ", blockIndex=" + blockIndex
                    + ", sendTimestamp=" + sendTimestamp + ", acked=" + acked + "]";
        }

    }

    /**
     * The Enum AckState used to indicate the status of the message ack record
     * object -- can be free, used or ACKed
     */
    public enum AckState {
        FREE, ACKED, USED;
    }

    /**
     * The Class StructMessageBlockAcks holds the count of messages in a
     * particular {@link MessageBlock} that are pending ACKs. Used for deciding
     * whether a {@link MessageBlock} is a candidate for GC or not.
     */
    public static final class StructMessageBlockAcks {
        /** Message block id */
        public String blockId;
        /**
         * timestamp whenever this block contents are updated (message added or
         * messages acked)
         */
        public long timestamp;
        /** number of messages pending ACKs */
        public long count = 0;
    }

    /**
     * StructSubscriberConfig represents the {@link Subscriber} related configs
     * that are useful for allocating ack cache. This corresponds to storing the
     * subscriber name, ack cache max size, ack cache used size, and ack timeout
     */
    public static final class StructSubscriberConfig {
        /** subscriber name */
        public String subscriber;
        /**
         * ack cache max size -- corresponds to subscriber.ack.cache.size *
         * subscriber.batch.size
         */
        public int maxSize;
        /** ack cache used currently */
        public int usedSize = 0;
        /** ack timeout */
        public long ackTimeout;
    }

    /**
     * ReusableAckRecord -- Reusable wrapper around {@link MessageAckRecord}
     * which lets the {@link MessageAckRecord} be reused. Implements
     * {@link Reusable} interface.
     */
    public static final class ReusableAckRecord implements Reusable<MessageAckRecord> {

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
