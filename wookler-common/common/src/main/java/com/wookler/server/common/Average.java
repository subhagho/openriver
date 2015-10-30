/*
 *
 *  * Copyright 2014 Subhabrata Ghosh
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.wookler.server.common;

import org.joda.time.DateTime;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 05/08/14
 */
public class Average extends AbstractMeasure {
    private AtomicLong value = new AtomicLong(0);
    private AtomicLong count = new AtomicLong(0);

    public long value() {
        return value.get();
    }

    public long count() {
        return count.get();
    }

    public double average() {
        if (count.get() > 0) {
            return ((double) value.get()) / count.get();
        }
        return 0.0;
    }

    public Average add(long value, long count) {
        this.value.addAndGet(value);
        this.count.addAndGet(count);
        return this;
    }

    public Average add(long value) {
        return add(value, 1);
    }

    @Override
    public AbstractMeasure add(AbstractMeasure measure) {
        if (measure instanceof Average) {
            add(((Average) measure).value(), ((Average) measure).count());
        }
        return this;
    }

    @Override
    public AbstractMeasure clear() {
        this.value.set(0);
        this.count.set(0);

        return this;
    }

    @Override
    public String toString() {
        return String.format("{%s: WINDOW=%s, AVERAGE=%f, VALUE=%d, COUNT=%d}", getClass().getSimpleName(),
                new DateTime(window).toString(_WINDOW_DATE_FORMAT_), average(), value.get(), count.get());
    }

    @Override
    public AbstractMeasure copy() {
        Average m = new Average();
        m.window = window;
        m.value = new AtomicLong(value());
        m.count = new AtomicLong(count());

        return m;
    }
}
