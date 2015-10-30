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

import com.wookler.server.common.TimeWindow;
import junit.framework.TestCase;
import org.joda.time.DateTime;

import java.util.concurrent.TimeUnit;

public class Test_TimeWindow extends TestCase {
    private static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";

    public void testGetTimeWindow() throws Exception {
        doTestMilliTimeWindow();
        doTestSecTimeWindow();
        doTestMinTimeWindow();
        doTestHourTimeWindow();
        doTestDayTimeWindow();
    }

    private void doTestMilliTimeWindow() throws Exception {
        TimeWindow tw = new TimeWindow();
        tw.granularity(TimeUnit.MILLISECONDS);
        tw.resolution(20);

        long time = System.currentTimeMillis();
        DateTime dt = new DateTime(time);

        System.out.println("Date : " + dt.toString(TIME_FORMAT));
        long tr = tw.windowStart(time);
        DateTime dr = new DateTime(tr);
        System.out.println("Time Milli Window: " + dr.toString(TIME_FORMAT));

    }

    private void doTestSecTimeWindow() throws Exception {
        TimeWindow tw = new TimeWindow();
        tw.granularity(TimeUnit.SECONDS);
        tw.resolution(15);

        long time = System.currentTimeMillis();
        DateTime dt = new DateTime(time);

        System.out.println("Date : " + dt.toString(TIME_FORMAT));
        long tr = tw.windowStart(time);
        DateTime dr = new DateTime(tr);
        System.out.println("Time Seconds Window: " + dr.toString(TIME_FORMAT));

    }

    private void doTestMinTimeWindow() throws Exception {
        TimeWindow tw = new TimeWindow();
        tw.granularity(TimeUnit.MINUTES);
        tw.resolution(45);

        long time = System.currentTimeMillis();
        DateTime dt = new DateTime(time);

        System.out.println("Date : " + dt.toString(TIME_FORMAT));
        long tr = tw.windowStart(time);
        DateTime dr = new DateTime(tr);
        System.out.println("Time Minutes Window: " + dr.toString(TIME_FORMAT));

    }

    private void doTestHourTimeWindow() throws Exception {
        TimeWindow tw = new TimeWindow();
        tw.granularity(TimeUnit.HOURS);
        tw.resolution(36);

        long time = System.currentTimeMillis();
        DateTime dt = new DateTime(time);

        System.out.println("Date : " + dt.toString(TIME_FORMAT));
        long tr = tw.windowStart(time);
        DateTime dr = new DateTime(tr);
        System.out.println("Time Hours Window: " + dr.toString(TIME_FORMAT));

    }

    private void doTestDayTimeWindow() throws Exception {
        TimeWindow tw = new TimeWindow();
        tw.granularity(TimeUnit.DAYS);
        tw.resolution(7);

        long time = System.currentTimeMillis();
        DateTime dt = new DateTime(time);

        System.out.println("Date : " + dt.toString(TIME_FORMAT));
        long tr = tw.windowStart(time);
        DateTime dr = new DateTime(tr);
        System.out.println("Time Days Window: " + dr.toString(TIME_FORMAT));

    }
}