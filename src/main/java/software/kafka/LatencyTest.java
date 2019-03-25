/**
 * This program tests performance of Kafka across hosts on a network.
 */

package software.kafka;

import java.io.IOException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientStats;
import org.voltdb.client.ClientStatsContext;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.NullCallback;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import com.google.common.io.Resources;

import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class LatencyTest implements org.apache.kafka.clients.producer.Callback {

	private static final int BATCH_SIZE = 1000;//1000000;
	private static final int BUCKET_SIZE = 1000;
	private static final int BUCKET_COUNT = 10;

	private final StringBuilder sb = new StringBuilder();
	private long writeStart = 0;
	private long readStart = 0;

	public String mode;
	public String topic;
	public int size;
	public int sleep;
	public int numberOfMessages;
	public int chunkSize;

	private Client voltDBClient;

	static ClientStatsContext periodicStatsContext;
	static ClientStatsContext fullStatsContext;
	long benchmarkStartTS;
	
	private void runReadTest() throws IOException {
		System.out.println("Running read test");
		readStart = 0;
		long readCount = 0;
		long latencyTotal = 0;
		long latencyMin = 0;
		long latencyMax = 0;
		long latencyBuckets[] = new long[BUCKET_COUNT + 1];

		// read
		KafkaConsumer<String, String> consumer;
		try (InputStream props = Resources.getResource("consumer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			if (properties.getProperty("group.id") == null) {
				properties.setProperty("group.id", "group-" + new Random().nextInt(100000));
			}
			consumer = new KafkaConsumer<>(properties);
		}
		consumer.subscribe(Arrays.asList(this.topic));
		consumer.seekToEnd(consumer.assignment());

		int lowerBound = 0;

		readStart = System.nanoTime(); 

		while (readCount < numberOfMessages) {
			this.voltDBClient.callProcedure(new NullCallback(), "insert_from_dummy_data", lowerBound, lowerBound + chunkSize);
			// read records with a short timeout. If we time out, we don't really care.
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0).toMillis());

			int recordCount = records.count();
			if (recordCount == 0 && readCount < numberOfMessages) {
				continue;
			}
			System.out.println("Count "+ records.count());

			for (ConsumerRecord<String, String> record : records) {
				long latency = (System.nanoTime() - readStart) / 1000; // in microseconds one-way
				if (latencyTotal == 0) {
					latencyTotal = latency;
					latencyMin = latency;
					latencyMax = latency;
				} else {
					latencyTotal += latency;
					latencyMin = (latency < latencyMin) ? latency : latencyMin;
					latencyMax = (latency > latencyMax) ? latency : latencyMax;
				}

				int bucket = (int) (latency / BUCKET_SIZE);
				latencyBuckets[bucket > BUCKET_COUNT ? BUCKET_COUNT : bucket]++;

				++readCount;
				if (readCount % BATCH_SIZE == 0) {
					long rate = readRate();
					System.out.println(String.format("read (count: %,d, rate: %,d msg/s)", readCount, rate));
					System.out.println(String.format("latency (average: %,dus, min: %,dus, max: %,dus)",
							+latencyTotal / BATCH_SIZE, latencyMin, latencyMax));
					System.out.println("       range         %      samples");
					for (int i = 0; i < BUCKET_COUNT; i++) {
						System.out.println(String.format("  < %,6dus    %6.2f   %,10d", BUCKET_SIZE * (i + 1),
								(100.0 * latencyBuckets[i]) / BATCH_SIZE, latencyBuckets[i]));
					}
					System.out.println(String.format("  > %,6dus    %6.2f   %,10d", BUCKET_SIZE * BUCKET_COUNT,
							(100.0 * latencyBuckets[BUCKET_COUNT]) / BATCH_SIZE, latencyBuckets[BUCKET_COUNT]));

					latencyTotal = 0;
					latencyMin = 0;
					latencyMax = 0;
					for (int i = 0; i <= BUCKET_COUNT; i++) {
						latencyBuckets[i] = 0;
					}
				}
			}

			lowerBound = lowerBound + this.chunkSize;
		}
		consumer.close();
	}

	private long readRate() {
		long now = System.nanoTime();
		long rate = (BATCH_SIZE * 1000000) / (now - readStart);
		readStart = now;

		return rate;
	}
	
	private final int NUM_OF_TASKS = 6;
	private ScheduledThreadPoolExecutor m_taskExecutor = new ScheduledThreadPoolExecutor(NUM_OF_TASKS);
	
	private class WriteToKafkaTask implements Callable<Void> {

		private int startId, endId;
		private KafkaProducer<String, String> producer;
		byte[] writeData;
		
		public WriteToKafkaTask(int startId, int endId, byte[] writeData, KafkaProducer<String, String>  producer) {
			this.startId = startId;
			this.endId = endId;
			this.producer = producer;
			this.writeData = writeData;
		}
		
		@Override
		public Void call() throws Exception {
			for(int i=startId; i<endId; i++) {
				long sendStartTime = System.currentTimeMillis();
				producer.send(
						new ProducerRecord<String, String>(topic, sendStartTime + "," + i + "," + writeData),
						LatencyTest.this);
			}
			return null;
		}
	}

	private void runWriteTest() throws IOException, ProcCallException {
		System.out.println("Running write test");

		// set up the producer
		KafkaProducer<String, String> producer = null;
		try (InputStream props = Resources.getResource("producer.props").openStream()) {
			Properties properties = new Properties();
			properties.load(props);
			producer = new KafkaProducer<>(properties);
		} catch (IOException e) {
			e.printStackTrace();
		}

		byte[] writeData = new byte[size];
		for (int i = 0; i < size; ++i) {
			writeData[i] = (byte) (i & 0xFF);
		}

		benchmarkStartTS = System.currentTimeMillis();
		schedulePeriodicStats();
		
		int idsPerTask = numberOfMessages/NUM_OF_TASKS;
		
		for(int x=0; x<numberOfMessages; x+=idsPerTask) {
			int y = x + idsPerTask - 1;
			m_taskExecutor.submit(new WriteToKafkaTask(x, y, writeData, producer));
		}
	}

	/**
	 * Create a Timer task to display performance data on the Vote procedure
	 * It calls printStatistics() every displayInterval seconds
	 */
	public void schedulePeriodicStats() {
		Timer timer = new Timer();
		TimerTask statsPrinting = new TimerTask() {
			@Override
			public void run() { printStatistics(); }
		};
		timer.scheduleAtFixedRate(statsPrinting, 5000, 5000);
	}

	public synchronized void printStatistics() {
		ClientStats stats = periodicStatsContext.fetchAndResetBaseline().getStats();
		long time = Math.round((stats.getEndTimestamp() - benchmarkStartTS) / 1000.0);

		System.out.printf("%02d:%02d:%02d ", time / 3600, (time / 60) % 60, time % 60);
		System.out.printf("Throughput %d/s, ", stats.getTxnThroughput());
		System.out.printf("Aborts/Failures %d/%d",
				stats.getInvocationAborts(), stats.getInvocationErrors());
		System.out.printf(", Avg/95%% Latency %.2f/%.2fms", stats.getAverageLatency(),
				stats.kPercentileLatencyAsDouble(0.95));
		System.out.printf("\n");
	}

	private long writeRate(long now) {
		long rate = (BATCH_SIZE * 1000) / (now - writeStart);
		writeStart = 0;
		return rate;
	}
	public static void main(String[] args) throws Exception {

		if (args.length < 1) {
			System.out.println("  Usage: <mode[READ,WRITE]> <topic> [size=200] [number of messages=1000] [chunk size=1000] [sleep=0] [voltdb url]");
			System.out.println("  Options: <mode>  - Test mode write or read");
			System.out.println("  <topic>  - Topic used to write or read messages");
			System.out.println("  <size>  - message size in bytes (optional, default is 200)");
			System.out.println("  <number of messages>  - Total number of messages read or written");
			System.out.println("  <chunk size>  - number of messages exported at a time");
			System.out
			.println("         <sleep> - sleep time between messages in nanoseconds (optional, default is 0)");
			System.out.println("  <voltdb url>  - url of the voltdb");
			return;
		}

		LatencyTest test = new LatencyTest();
		test.mode = args[0];
		test.topic = args[1];
		test.size = args.length > 2 ? Integer.parseInt(args[2]) : 200;
		test.numberOfMessages = args.length > 3 ? Integer.parseInt(args[3]) : 1000;
		test.chunkSize =  args.length > 4 ? Integer.parseInt(args[4]) : 1000;
		test.sleep = args.length > 5 ? Integer.parseInt(args[5]) : 0;
		String voltDBServers = args.length > 6 ? args[6] : "localhost";
		System.out.println( "mode "+test.mode +" topic "+ test.topic  +" size "+ test.size +" number of messages "+ test.numberOfMessages  +" chunk size "+ test.chunkSize  +" sleep "+ test.sleep  +" voltdb url "+ voltDBServers);
		// create a java client instance using default settings
		Client client = ClientFactory.createClient();
		periodicStatsContext = client.createStatsContext();
        fullStatsContext = client.createStatsContext();
		// connect to each server listed (separated by commas) in the first argument
		String[] serverArray = voltDBServers.split(",");
		for (String server : serverArray) {
			client.createConnection(server);
		}

		test.voltDBClient = client;

		switch (test.mode.toUpperCase()) {
		case "READ":
			test.runReadTest();
			break;
		case "WRITE":
			test.runWriteTest();
			break;
		default:
		}
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {}
}