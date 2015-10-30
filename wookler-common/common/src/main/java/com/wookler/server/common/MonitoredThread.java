package com.wookler.server.common;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * @author Subho Ghosh (subho dot ghosh at outlook.com)
 * @created 06/08/14
 */
public class MonitoredThread extends Thread {

    public MonitoredThread() {

    }

    public MonitoredThread(Runnable target) {
        super(target);

    }

    public MonitoredThread(ThreadGroup group, Runnable target) {
        super(group, target);
    }

    public MonitoredThread(String name) {
        super(name);
    }

    public MonitoredThread(ThreadGroup group, String name) {
        super(group, name);
    }

    public MonitoredThread(Runnable target, String name) {
        super(target, name);
    }

    public MonitoredThread(ThreadGroup group, Runnable target, String name) {
        super(group, target, name);
    }

    public MonitoredThread(ThreadGroup group, Runnable target, String name, long stackSize) {
        super(group, target, name, stackSize);
    }

    public ThreadInfo getThreadInfo() {
        ThreadMXBean tmb = ManagementFactory.getThreadMXBean();
        return tmb.getThreadInfo(getId());
    }
}
