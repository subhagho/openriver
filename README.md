#RIVER - In-Process Queue

***

##Background
***

While building a platform for ingesting streaming events from different sources into our data platform we realised the need to have some kind of locally persisted queues at each handover point to provide a high level of data reliability. Events were being routed via multiple tenants and networks, each of which had its own availability characteristics, causing some loss of data at various of these touch-points. To start with we experimented using a JMS messaging framework as the relay for collecting the events, but there were a few issues which caused us to rethink this approach.

1. Scenario where the messaging infrastructure or the connectivity to it was down.
2. There can be multiple hops between the origin and the final destination and having a JMS queue at each message hand-off point would significantly increase latency and cost.
3. Building a messaging infrastructure to support an ingest volume of billions of events per day would be a significant operational overhead.

To overcome these issues we built a persisted local queuing library which could be used by publishing applications and all collections points to first persist the events in a local store and then forward it downstream (store & forward). This would allow each touch-point to be able to manage failure and recovery independently and since the data was being stored locally within the context of the application there wouldn’t be any external dependencies.

For the first release we developed a simple [Chronicle](https://github.com/OpenHFT/Java-Chronicle) based in-process queue that provided the basic reliability required to ensure that events weren’t dropped in the pipeline. Soon we realised that we could do so much more with this Chronicle based queue and hence expanded the initial prototype to include more messaging constructs such as multiple subscriber channels, support [Reactor Pattern](http://en.wikipedia.org/wiki/Reactor_pattern) based message processors, etc. We have also developed a REST based service tier to provide out-of-process messaging service. The REST server can be used as a message relay and provide a standalone messaging service. A corresponding REST client is also available to send/consume messages from the messaging service. We have not tried to create a JMS complaint messaging service as the primary objective of this library is to support high-volume event processing and not transactional applications. Hence the library if optimised for performance and provides a highly reliable infrastructure for event processing. 

##Design
***

<pre>
<code>
                                                            - -- Message Processor (Reactor)
                                                            - ...
                                                            -
        Publisher(s) -- Message Queue -- Chronicle Store -- - -- Message Processor (Reactor)
                                                            -
                                                            - ...
                                                            - -- Pull Subscriber(s)
</code>
</pre>

The design of the queue framework is quite simple. It uses Chronicle as a persistent store and adds all messages published to this. Subscribers pickup messages from the queue and process them downstream. Each subscriber maintains its own read pointer into this queue, hence the messages are not duplicated. Subscribers can require ACK in which case resend is enabled for messages that are not ACK'd in time. Features we have added on top of the base Chronicle library are as follows:

1. Recycle Chronicle data blocks based on time or size.
2. Provide messaging semantics (queue/publish/subscribe APIs).
3. Provide multiple subscribers which can operate at different speeds on these queues.
4. Ability to control data reliability via ACK and resend.
5. Define reactors to handle message processing. (Explained in detail later)

A configuration framework is provided to be able to control various aspects of the queue setup. Detailed documentation on the configuration parameters provided in the wiki.

##Performance
***

Given that event beacons are fired from online applications, it was very important for us to optimise the performance on the publish side so as to minimise the overhead to live transactions. Towards this end RIVER synchronisation (all message operations are thread safe) are optimised for the publishers at the expense of the consumers. We did evaluate concepts like Ring Buffers and Disruptors, but the challenge with both is that the performance on the publisher side is optimal only in single-threaded scenarios, as RIVER is often embedded in a container the assumption was that we would support thread safe operations. Another challenge with the ring buffer concept is the max size imposed on the number of messages in the buffers that would result in events being dropped if the backlog was to be larger than the ring buffer size. There are ways to circumventing this limitation, but given the first assumption we decided to incur the overhead introduced by locking.

Currently the performance of the queue is limited by IO bandwidth only. As in the queues can accept as many messages as can be persisted into the Chronicle queues per second. 

##Benchmark


###System:


The benchmark numbers were based on tests run on a MacBook Pro with SSD drive.

#####Processor
2.6 GHz Intel Core i7

#####Memory
16 GB 1600 MHz DDR3

The average size of the messages is about 1KB. Throughput is very sensitive to the message size as the writes max out the IO bandwidth. Reducing the message size to 256B increases the throughput by almost 4X. 

###_Publisher Performance_
***

####Single Publisher Thread

<pre>
    [TEST-PRODUCER-0] Published [4504360] messages : [AVG:0.004492][TOT:20232], [ELAPSED:29324]
</pre>

<pre>
Total Messages published    : 4504360
Elapsed time                : 29324 milliseconds
Number of messages/second   : 153606
</pre>

####Multiple Publisher Thread

<pre>
    [TEST-PRODUCER-0] Published [4504360] messages : [AVG:0.026541][TOT:119549], [ELAPSED:158412]
    [TEST-PRODUCER-1] Published [4504360] messages : [AVG:0.026467][TOT:119217], [ELAPSED:158412]
    [TEST-PRODUCER-2] Published [4504360] messages : [AVG:0.026530][TOT:119502], [ELAPSED:158411]
    [TEST-PRODUCER-3] Published [4504360] messages : [AVG:0.026546][TOT:119574], [ELAPSED:158411]
</pre>

<pre>
Total Messages published    : 18017440
Elapsed time                : 158412 milliseconds
Number of messages/second   : 113738
</pre>

Note that the elapsed time also includes the reading of the message records from a file. Based on the average message publish time the numbers are even better.

###Single Publisher Thread


<pre>
AVG                         : 0.004492
Number of messages/second   : 222618
</pre>

####Multiple Publisher Thread


<pre>
AVG                         : 0.026541
Number of messages/second   : 150710
</pre>

As is evident from the Single/Multi publisher throughput results, it is very apparent that locking does play a significant part. There is a 33% degradation in the performance between the two scenarios. 

###_Consumer Performance_
***

Consumer performance is subject to the complexity of the **_Processor_**  implementation. In the sample provided, the consumers just write the messages to a file.

<pre>
    [TEST-CONSUMER-1] Processed [1204199] records : [AVG:0.013705][TOT:16503]
    [TEST-CONSUMER-2] Processed [1204224] records : [AVG:0.013680][TOT:16474]
    [TEST-CONSUMER-3] Processed [1204224] records : [AVG:0.013570][TOT:16341]
</pre>

<pre>
Total Messages consumed     : 3612637
Elapsed time                : 16503 milliseconds
Number of messages/second   : 218908
</pre>

##Examples
***

The [example](/examples) project contains various implementations that highlight the features of this library. The [Main](/examples/src/main/java/com/wookler/river/examples/Main.java) class in this project can be used to launch any of the provided examples. The examples also provide details around the configurations for using the queue(s) and subscriber modes.

Running the examples:

```
java -cp river-examples-1.0-SNAPSHOT-jar-with-dependencies.jar com.wookler.river.examples.Main --config [configfile path] --example com.wookler.river.examples.MessagePullSample 
```

###Environment Setup

The queuing library also includes certain utility frameworks for thread management, processing counters, etc. These utilities are in the [common](/common) project. The core messaging framework is dependent on these to be setup as part of the execution environment. There is a [environment setup singleton](/common/src/main/java/com/wookler/river/common/Env.java) provided that can be used
to perform all the required setup function. This singleton needs to be invoked/initialized before the queue definitions can be loaded.

#####Snippet

```java

    // Setup the execution environment.
    // configfile - Path to the configuration file.
    // SampleConstants.CONFIG_PATH - XPath root under which the configurations are defined.
    Env.create(configfile, SampleConstants.CONFIG_PATH);
```

#####Configuration

```xml

            <!-- Execution Environment configuration -->
            <env>
                <monitor>
                    <params>
                        <!-- Application name [required]-->
                        <param name="app.name" value="TEST-RIVER"/>
                        <!-- Application Counter recycle window [required] -->
                        <param name="monitor.window.recycle" value="30ss"/>
                        <!-- Counter logging frequency -->
                        <param name="monitor.frequency.write" value="1mm"/>
                    </params>
                    <!-- Implementation class for logging application counters -->
                    <counter class="com.wookler.river.examples.utils.LogCounterLogger"/>
                    <!-- Implementation class for logging thread heartbeats -->
                    <heartbeat class="com.wookler.river.examples.utils.LogHeartbeatLogger"/>
                </monitor>
                <!-- Background managed task manager -->
                <task-manager name="TEST-RIVER-TM">
                    <params>
                        <!-- Managed task execution pool size.-->
                        <param name="executor.pool.size" value="1"/>
                    </params>
                </task-manager>
            </env>
```

###Core Utility Framework

This section gives a brief overview of some utility/framework classes available in the [common](/common) project that are used extensively.

####Application Counters

Application Counters are available to monitor Counts or Averages. The counter framework can be used to define arbitrary counters for monitoring application progress and/or performance. Counters need to be registered on initialization 
and then can be used to capture progress. Both simple (non-locking) and concurrent counters are available. A utility class [Monitoring](/common/src/main/java/com/wookler/river/common/utils/Monitoring.java) is available to make registering and updating counters
simpler.

#####Registering Counters:

```java

            /**
             * Create a new instance of a counter.
             *
             * @param global    - Is Global? (Synchronized)
             * @param namespace - Counter namespace.
             * @param name      - Counter name.
             * @param type      - Measure type.
             * @param mode      - Production or Debug counter.
             * @return - New instance of a counter.
             */
            public static AbstractCounter create(boolean global, String namespace, String name,
                                                 Class<? extends AbstractMeasure> type,
                                                 AbstractCounter.Mode mode) {
                AbstractCounter c = null;
                try {
                    TimeWindow window = Monitor.get().timewindow();
        
                    if (global) {
                        c = new ConcurrentCounter(window, type).namespace(namespace).name(name).mode(mode);
                        addGlocalCounter(c);
                    } else {
                        c = new Counter(window, type).namespace(namespace).name(name).mode(mode);
                        addLocalCounter(c);
                    }
                } catch (Monitor.MonitorException me) {
                    debug(Monitoring.class, me);
                }
                return c;
            }
            
            AbstractCounter c =
                    Monitoring.create(true, Constants.MONITOR_NAMESPACE, Constants.MONITOR_COUNTER_ADDS, Count.class,
                                             AbstractCounter.Mode.PROD);
            if (c != null) {
                counters.put(Constants.MONITOR_COUNTER_ADDS, new String[]{c.namespace(), c.name()});
            }
```

####Counter Sample

Sample output of a simple counter logging implementation to write the counters to the log file.

    Application Counters:
<pre>
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[AVERAGE] river.counters.block.time.read : [[DELTA: {Average: WINDOW=2014-09-25 16:01:35.499, AVERAGE=0.000000, VALUE=0, COUNT=0}][PERIOD: {Average: WINDOW=2014-09-25 16:02:30.000, AVERAGE=0.000000, VALUE=0, COUNT=0}][TOTAL: {Average: WINDOW=2014-09-25 16:01:35.499, AVERAGE=0.000000, VALUE=0, COUNT=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[AVERAGE] river.counters.block.time.write : [[DELTA: {Average: WINDOW=2014-09-25 16:01:35.499, AVERAGE=0.000200, VALUE=17753, COUNT=88590968}][PERIOD: {Average: WINDOW=2014-09-25 16:02:30.000, AVERAGE=0.000200, VALUE=17753, COUNT=88590973}][TOTAL: {Average: WINDOW=2014-09-25 16:01:35.499, AVERAGE=0.000000, VALUE=0, COUNT=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[AVERAGE] river.counters.processor.time : [[DELTA: {Average: WINDOW=2014-09-25 16:01:35.593, AVERAGE=0.007324, VALUE=32990, COUNT=4504360}][PERIOD: {Average: WINDOW=2014-09-25 16:02:30.000, AVERAGE=0.007324, VALUE=32990, COUNT=4504360}][TOTAL: {Average: WINDOW=2014-09-25 16:01:35.593, AVERAGE=0.000000, VALUE=0, COUNT=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.processor.messages : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=4504360}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=4504360}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.processor.exceptions : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=0}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=0}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.processor.success : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=4504360}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=4504360}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.processor.failed : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=0}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=0}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.593, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[AVERAGE] river.counters.queue.time.read : [[DELTA: {Average: WINDOW=2014-09-25 16:01:35.587, AVERAGE=0.010116, VALUE=45569, COUNT=4504647}][PERIOD: {Average: WINDOW=2014-09-25 16:02:30.000, AVERAGE=0.010116, VALUE=45569, COUNT=4504647}][TOTAL: {Average: WINDOW=2014-09-25 16:01:35.587, AVERAGE=0.000000, VALUE=0, COUNT=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.queue.acks : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.587, VALUE=0}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=0}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.587, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[AVERAGE] river.counters.queue.time.add : [[DELTA: {Average: WINDOW=2014-09-25 16:01:35.587, AVERAGE=0.004705, VALUE=21195, COUNT=4504360}][PERIOD: {Average: WINDOW=2014-09-25 16:02:30.000, AVERAGE=0.004705, VALUE=21195, COUNT=4504360}][TOTAL: {Average: WINDOW=2014-09-25 16:01:35.587, AVERAGE=0.000000, VALUE=0, COUNT=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.queue.adds : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.587, VALUE=4504360}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=4504360}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.587, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.queue.reads : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.587, VALUE=4504360}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=4504360}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.587, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.queue.storeTEST-RIVER-PULL.adds : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.531, VALUE=4504360}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=4504360}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.531, VALUE=0}]]}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {[COUNT] river.counters.queue.storeTEST-RIVER-PULL.reads : [[DELTA: {Count: WINDOW=2014-09-25 16:01:35.531, VALUE=4504360}][PERIOD: {Count: WINDOW=2014-09-25 16:02:30.000, VALUE=4504360}][TOTAL: {Count: WINDOW=2014-09-25 16:01:35.531, VALUE=0}]]}
</pre>

    Thread Heartbeats
<pre>
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {{THREAD: ID=16, NAME=CONSUMER-TEST-CONSUMER-3, STATE=TIMED_WAITING} CPUTIME=39010470, USERTIME=34969175}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {{THREAD: ID=18, NAME=POOL-1-TaskManager-THREAD-1, STATE=RUNNABLE} CPUTIME=131475, USERTIME=117613}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {THREAD: ID=13, NAME=PRODUCER-TEST-PRODUCER-0, STATE=TERMINATED}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {{THREAD: ID=14, NAME=CONSUMER-TEST-CONSUMER-1, STATE=RUNNABLE} CPUTIME=28008074, USERTIME=24007692}
[APP INFO: [APP NAME:TEST-RIVER][IP:10.242.83.186][HOSTNAME:10.242.83.186][START TIME:1411641094981]] {{THREAD: ID=15, NAME=CONSUMER-TEST-CONSUMER-2, STATE=WAITING} CPUTIME=19259811, USERTIME=15274813}
</pre>

####[Monitored Threads](/common/src/main/java/com/wookler/river/common/MonitoredThread.java)

Threads launched by the core framework provides the hooks to be monitored via heartbeats. The [framework monitor](/common/src/main/java/com/wookler/river/common/Monitor.java)
periodically polls and logs the status of these registered threads. This framework can also be used by the application in case it needs to launch and manage threads.


###Queue Setup and Configuration

This provides snippets for setting up and configuring the queue(s).
 
#####Snippet

```java
        
        private MessageQueue<String> queue = new MessageQueue<String>();
        
        // Find the Queue configuration node in the tree.
        ConfigNode node = cp.search(SampleConstants.CONFIG_PATH_QUEUE);
        // Setup and start the queue.
        configQueue(node);
                    
        // Configure and start the queue.
        // node - Configuration node for the queue setup.
        private void configQueue(ConfigNode node) throws Exception {
            if (node == null)
                throw new Exception("Cannot find queue node. [path=" + SampleConstants.CONFIG_PATH_QUEUE + "]");
            queue.configure(node);
            queue.start();
        }
```

#####Configuration

```xml

            <!-- Define the queue configuration -->
            <queue name="TEST-RIVER-EXEC">
                <params>
                    <!-- Queue Lock Timeout - Used to acquire read locks on the queue. -->
                    <param name="queue.lock.timeout" value="100"/>
                    <!-- Byte transformation implementation - used when serializing/de-serializing messages to/from the queue -->
                    <param name="queue.message.converter" value="com.wookler.river.examples.utils.StringMessageConverter"/>
                    <!-- Cache configuration file - EHCache used to record messages for ACK/resend. -->
                    <param name="ehcache.config" value="examples/src/main/resources/example-ehcache.xml"/>
                    <!-- Directory path where Chronicle files are created. -->
                    <param name="queue.directory" value="/tmp/river/test"/>
                    <!-- Reload data from Chronicle files if any present (default is true)-->
                    <param name="queue.onstart.reload" value="false"/>
                </params>
                <!--
                    Strategy for re-cycling Chronicle data files.
                    Strategy Options : Size Based or Time Based
                -->
                <recycle class="com.wookler.river.core.SizeBasedRecycle">
                    <params>
                        <!-- Size of the Chronicle (#of records not byte size) -->
                        <param name="recycle.size" value="1000000"/>
                    </params>
                </recycle>
                <!-- Backup Chronicle data files once all the messages have been consumed. Backup compresses the files. (OPTIONAL)-->
                <backup>
                    <params>
                        <!-- Directory to backup the files at.  -->
                        <param name="backup.directory" value="/tmp/river/test/backup"/>
                        <!-- Period to retain these backup files for.-->
                        <param name="backup.retention" value="10mm"/>
                    </params>
                </backup>
            </queue>

```
###[Reactor Example](/examples/src/main/java/com/wookler/river/examples/ReactorSample.java)

This example implements **_message processors_** embedded within the subscribers to react to and handle incoming events.

