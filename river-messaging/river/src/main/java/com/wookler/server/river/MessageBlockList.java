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

import com.wookler.server.common.utils.LogUtils;
import net.openhft.chronicle.ChronicleConfig;

import java.util.concurrent.locks.ReentrantLock;

/**
 * A linked list wrapper for the {@link MessageBlock}.
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 18/08/14
 */
public class MessageBlockList {
    /** Default number of empty blocks to be initialized */
    private static final int EMPTY_BLOCK_SIZE = 25;
    /** head of the list */
    private MessageBlock head;
    /** tail of the list */
    private MessageBlock tail;
    /** ptr to the block that is being written to */
    private MessageBlock writer;
    /** size of the list */
    private int size;
    /** lock */
    private ReentrantLock lock = new ReentrantLock();
    /** Recycle strategy */
    private RecycleStrategy strategy;
    /** storage manager */
    private MessageStoreManager parent;
    /** empty blocks to be initialized */
    private int emptyBlockSize = EMPTY_BLOCK_SIZE;
    /** Chronicle config size */
    private ChronicleConfig cc;

    /**
     * Instantiates a new message block list with the specified
     * {@link RecycleStrategy} and {@link ChronicleConfig}
     *
     * @param strategy
     *            the recycle strategy
     * @param parent
     *            the message store
     * @param cc
     *            the chronicle config
     */
    public MessageBlockList(RecycleStrategy strategy, MessageStoreManager parent, ChronicleConfig cc) {
        this.strategy = strategy;
        this.parent = parent;
        this.cc = cc;
    }

    /**
     * Gets the lock on the MessageBlockList
     *
     * @return the reentrant lock
     */
    public ReentrantLock lock() {
        return lock;
    }

    /**
     * Add a new block to the end (tail) of the queue. The new block is assumed
     * to be the block currently written to.
     *
     * @param block
     *            - New block to add.
     * @return - self.
     */
    public MessageBlockList add(MessageBlock block) {
        lock.lock();
        try {
            block.next(null);
            if (head == null) {
                head = block;
                tail = head;
            } else {
                tail.next(block);
                block.previous(tail);
                tail = tail.next();
            }
            size++;

            return this;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the Empty block count.
     *
     * @return the number of empty blocks
     */
    private int emptyBlockCount() {
        int count = 0;
        MessageBlock ptr = tail;
        while (ptr != null) {
            if (ptr.state() == EBlockState.Unsued) {
                count++;
            } else {
                break;
            }
            ptr = ptr.previous();
        }
        return count;
    }

    /**
     * Initializes emptyBlockSize number of empty blocks and adds them to the
     * tail of the MessageBlockList
     *
     * @throws MessageQueueException
     *             the message queue exception
     */
    public void initEmptyBlocks() throws MessageQueueException {
        int count = emptyBlockSize - emptyBlockCount();
        LogUtils.debug(getClass(), "Initializing [" + count + "] empty blocks...");
        MessageBlock ptr = tail;
        for (int ii = 0; ii < count; ii++) {
            MessageBlock b = parent.newblock(cc);
            lock.lock();
            try {
                add(b);
            } finally {
                lock.unlock();
            }
            ptr = ptr.next();
        }
    }

    /**
     * Set the empty MessageBlock size (number of empty message blocks in the
     * list)
     *
     * @param emptyBlockSize
     *            the empty block size
     * @return self
     */
    public MessageBlockList emptyBlockSize(int emptyBlockSize) {
        this.emptyBlockSize = emptyBlockSize;

        return this;
    }

    /**
     * Get the empty block size (number of empty message blocks in the list).
     *
     * @return the emptyBlockSize
     */
    public int emptyBlockSize() {
        return this.emptyBlockSize;
    }

    /**
     * Get the block being currently written to. (Always the tail block.)
     *
     * @return - Current write block.
     */
    public MessageBlock writeblock() throws MessageQueueException {
        if (writer == null) {
            MessageBlock ptr = tail;
            while (ptr != null) {
                if (ptr.state() == EBlockState.RW) {
                    writer = ptr;
                    break;
                }
                ptr = ptr.previous();
            }
        }
        if (strategy.recycle(writer)) {
            MessageBlock b = null;
            if (writer.next() != null && writer.next().state() == EBlockState.Unsued) {
                b = writer.next();
                b.openwriter();
            } else {
                LogUtils.warn(getClass(),
                        "Forcing creation of new block. As no blocks available for writing.");
                b = parent.newblock(cc);
                b.openwriter();
                add(b);
            }
            MessageBlock m = writer;
            m.closewriter();
            writer = b;
        }
        return writer;
    }

    /**
     * Return the head of the linked list (Oldest block).
     *
     * @return - Head of the list, NULL if empty.
     */
    public MessageBlock peek() {
        return head;
    }

    /**
     * Return the tail of the linked list (Newest block).
     *
     * @return - Tail of the list
     */
    public MessageBlock tail() {
        return tail;
    }

    /**
     * Remove the head block from the list and return it.
     *
     * @return - Head of the list, NULL if empty.
     */
    public MessageBlock poll() {
        lock.lock();
        try {
            MessageBlock b = head;

            head = head.next();
            head.previous(null);
            size--;

            return b;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Remove a specific block from the list.
     *
     * @param block
     *            - Block to remove.
     * @return - Removed Block.
     */
    public MessageBlock remove(MessageBlock block) {
        lock.lock();
        if (!block.canGC())
            return null;
        try {
            MessageBlock ptr = head;
            while (ptr != null) {
                if (ptr.equals(block)) {
                    MessageBlock prev = ptr.previous();
                    MessageBlock next = ptr.next();

                    if (prev != null) {
                        prev.next(next);
                        next.previous(prev);
                    }
                    size--;
                    if (ptr.equals(head)) {
                        head = next;
                    }
                    return ptr;
                }
                ptr = ptr.next();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Find a particular block in the list
     *
     * @param id
     *            the id of the block that needs to be found
     * @return the message block or null if the no block with the id is present
     */
    public MessageBlock find(String id) {
        MessageBlock ptr = head;
        while (ptr != null) {
            if (ptr.id().compareTo(id) == 0) {
                return ptr;
            }
            ptr = ptr.next();
        }
        return null;
    }

    /**
     * Get the current size of the list.
     *
     * @return - List size.
     */
    public int size() {
        return size;
    }
}
