/*
 * Copyright [2014] Subhabrata Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wookler.server.common.structs;

import org.junit.Test;

public class Test_StaticCircularList {

    @Test
    public void testPoll() throws Exception {
        StaticCircularList<Integer> list = new StaticCircularList<Integer>(10);
        for (int ii = 0; ii < list.size(); ii++) {
            list.add(ii);
        }

        Thread[] threads = new Thread[10];
        for (int ii = 0; ii < threads.length; ii++) {
            Runner r = new Runner(list, ii);
            Thread t = new Thread(r);
            t.start();
            threads[ii] = t;
        }
        for (int ii = 0; ii < threads.length; ii++) {

            threads[ii].join();
        }
    }

    private static class Runner implements Runnable {
        private StaticCircularList<Integer> list;
        private String name;
        private int index;

        public Runner(StaticCircularList<Integer> list, int index) {
            this.list = list;
            this.index = index;
            this.name = "[RUNNER:" + index + "]";
        }

        @Override
        public void run() {
            try {
                Thread.sleep(5000);
                for (int ii = 0; ii < 100; ii++) {
                    Integer i = list.poll();
                    System.out.println(String.format("[THREAD:%s] : Value = %d", name, i));
                    list.add(i);
                    Thread.sleep((index + 1 + ii) % 10);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}