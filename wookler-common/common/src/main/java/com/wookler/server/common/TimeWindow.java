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

import org.apache.commons.lang3.StringUtils;

import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Class represent a time window definition. Definition includes a Granularity (MILLI, SEC., MIN, etc.) and resolution
 * (multiplier for this granularity).
 *
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 02/08/14
 */
public class TimeWindow {
    private TimeUnit granularity;
    private int resolution;
    private long div = -1;

    /**
     * Get the Time Unit granularity for this Time Window.
     *
     * @return - Granularity Time Unit.
     */
    public TimeUnit granularity() {
        return granularity;
    }

    /**
     * Set the Time Unit granularity for this Time Window.
     *
     * @param granularity - Time Unit granularity
     * @return - seld.
     */
    public TimeWindow granularity(TimeUnit granularity) {
        this.granularity = granularity;

        // Invalidate the divisor as it's no longer correct.
        div = -1;

        return this;
    }

    /**
     * Get the resolution for this Time Window. Resolution is the multiplier used to multiply the Tine Unit to get the
     * actual window delta.
     *
     * @return - Resolution factor.
     */
    public int resolution() {
        return resolution;
    }

    /**
     * Set the resolution for this Time Window. Resolution is the multiplier used to multiply the Tine Unit to get the
     * actual window delta.
     *
     * @param resolution - Multiplier resolution.
     * @return - self.
     */
    public TimeWindow resolution(int resolution) {
        this.resolution = resolution;

        // Invalidate the divisor as it's no longer correct.
        div = -1;

        return this;
    }

    /**
     * Get the time window the specified timestamp falls into.
     *
     * @param timestamp - Input timestamp.
     * @return - Time window represented as the milliseconds value.
     * @throws TimeWindowException
     */
    public long windowStart(long timestamp) throws TimeWindowException {
        return ((timestamp / div()) * div());
    }

    /**
     * Get the create and end timestamps for the window the specified timestamp falls in.
     *
     * @param timestamp - Input timestamp.
     * @return - Long array[2] : array[1]=create time, array[2]=end time.
     * @throws TimeWindowException
     */
    public long[] window(long timestamp) throws TimeWindowException {
        long[] w = new long[2];

        w[0] = windowStart(timestamp);
        w[1] = w[0] + div();

        return w;
    }

    /**
     * Get the interval between the specified timestamp and the window end timestamp.
     *
     * @param timestamp - Input timestamp.
     * @return - Long delta.
     * @throws TimeWindowException
     */
    public long interval(long timestamp) throws TimeWindowException {
        long[] w = window(timestamp);

        return w[1] - timestamp;
    }

    /**
     * Get the window period in milliseconds.
     *
     * @return - The window period.
     * @throws TimeWindowException
     */
    public long period() throws TimeWindowException {
        return div();
    }

    /**
     * Default to string representation of this instance.
     *
     * @return - String representation.
     */
    @Override
    public String toString() {
        return String.format("TIME WINDOW: [GRANULARITY:%s][RESOLUTION:%d]", granularity.name(), resolution);
    }

    private long div() throws TimeWindowException {
        if (div < 0) {
            switch (granularity) {
                case MILLISECONDS:
                    div = 1;
                    break;
                case SECONDS:
                    div = 1000;
                    break;
                case MINUTES:
                    div = 1000 * 60;
                    break;
                case HOURS:
                    div = 1000 * 60 * 60;
                    break;
                case DAYS:
                    div = 1000 * 60 * 60 * 24;
                    break;
                default:
                    throw new TimeWindowException("Granularity not supported. [granularity = " + granularity.name() +
                                                          "]");
            }
            div *= resolution;
        }
        return div;
    }

    /**
     * Parse the passed string as Time window.
     * Time Window formats:
     * [VALUE][UNIT]
     * UNITS:
     * - ms : milliseconds
     * - ss : seconds
     * - mm : minutes
     * - hh : hours
     * - dd : days
     *
     * @param unit - Time Window string
     * @return - Parsed time window.
     * @throws TimeWindowException
     */
    public static TimeWindow parse(String unit) throws TimeWindowException {
        unit = unit.toUpperCase();
        Pattern p = Pattern.compile("(\\d+)(MS|SS|MM|HH|DD{1}$)");

        Matcher m = p.matcher(unit);
        if (m.matches()) {
            if (m.groupCount() >= 2) {
                String r = m.group(1);
                String g = m.group(2);
                if (!StringUtils.isEmpty(r) && !StringUtils.isEmpty(g)) {
                    TimeWindow tw = new TimeWindow();
                    tw.resolution(Integer.parseInt(r));
                    tw.granularity(timeunit(g));

                    return tw;
                }
            }
        }
        throw new TimeWindowException("Cannot parse Time Window from String. [string=" + unit + "]");
    }

    private static TimeUnit timeunit(String s) throws TimeWindowException {
        if (s.compareTo("MS") == 0) {
            return TimeUnit.MILLISECONDS;
        } else if (s.compareTo("SS") == 0) {
            return TimeUnit.SECONDS;
        } else if (s.compareTo("MM") == 0) {
            return TimeUnit.MINUTES;
        } else if (s.compareTo("HH") == 0) {
            return TimeUnit.HOURS;
        } else if (s.compareTo("DD") == 0) {
            return TimeUnit.DAYS;
        }
        throw new TimeWindowException("Invalid TimeUnit value. [string=" + s + "]");
    }
}
