/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package Streaming.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
//import org.apache.flink.table.api.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * A simple example that shows how to read from and write to Kafka. This will read String messages
 * from the input topic, parse them into a POJO type {@link KafkaEvent}, group by some key, and finally
 * perform a rolling addition on each key for which the results are written back to another topic.
 *
 * <p>This example also demonstrates using a watermark assigner to generate per-partition
 * watermarks directly in the Flink Kafka consumer. For demonstration purposes, it is assumed that
 * the String messages are of formatted as a (word,frequency,timestamp) tuple.
 *
 * <p>Example usage:
 * 	--input-topic test-input --output-topic test-output --bootstrap.servers localhost:9092 --zookeeper.connect localhost:2181 --group.id myconsumer
 */
public class Kafka010Example {

	public static void main(String[] args) throws Exception {
//		// parse input arguments
//		final ParameterTool parameterTool = ParameterTool.fromArgs(args);
//
//		if (parameterTool.getNumberOfParameters() < 5) {
//			System.out.println("Missing parameters!\n" +
//					"Usage: Kafka --input-topic <topic> --output-topic <topic> " +
//					"--bootstrap.servers <kafka brokers> " +
//					"--zookeeper.connect <zk quorum> --group.id <some id>");
//			return;
//		}

		Properties props = new Properties();
		props.put("bootstrap.servers", "meitudeMacBook-Pro-4.local:9092");
		props.put("zookeeper.connect","localhost:2181");
		props.put("group.id","test");

		props.put("metadata.fetch.timeout.ms","10000");
		props.put("metadata.max.age.ms","30000");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

//		env.getConfig().disableSysoutLogging();
//		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 10000));
//		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<KafkaEvent> input = env
				.addSource(
						new FlinkKafkaConsumer010<>(
								"foo",
								new KafkaEventSchema(),
								props)
								.assignTimestampsAndWatermarks(new CustomWatermarkExtractor()))
				.keyBy("word")
				.map(new RollingAdditionMapper());
		Table in = tEnv.fromDataStream(input,"timestamp1.rowtime,word,frequency");
//		tEnv.registerDataStream("wc", input, "timestamp1.rowtime,word,frequency");
		tEnv.registerTable("wc",in);
		final String sql = "select frequency,word,timestamp1 \n"
				+ "  from wc match_recognize \n"
				+ "  (\n"
				+ "	   order by  timestamp1 \n"
				+ "	   measures A.timestamp1 as timestamp1  ,\n"
				+ "	   A.word as  word ,\n"
				+ "	   A.frequency as  frequency \n"
				+ "	   ONE ROW PER MATCH \n"
				+ "    pattern (A B) \n"
				+ "    within interval '5' second \n"
				+ "    define \n"
				+ "      A AS A.word = 'bob' , \n"
				+ "      B AS B.word = 'kaka' \n"
				+ "  ) mr";
//
		Table table = tEnv.sqlQuery(sql);

		table.printSchema();
		tEnv.toAppendStream(table,Row.class).map((MapFunction<Row, KafkaEvent>) value -> {
//			System.out.println("============= > map");
			int frequency = (int) value.getField(0);
			String word = (String) value.getField(1);
			Timestamp timestamp1 = (Timestamp) value.getField(2);
			KafkaEvent kafkaEvent = new KafkaEvent();
			kafkaEvent.setFrequency(frequency);
			kafkaEvent.setTimestamp1(timestamp1.getTime());
			kafkaEvent.setWord(word);
			return kafkaEvent;
		}).addSink(new FlinkKafkaProducer010<>(
				"bar",
				new KafkaEventSchema(),
				props));

//		input.addSink(
//				new FlinkKafkaProducer010<>(
//						"bar",
//						new KafkaEventSchema(),
//					props));
//
		env.execute("Kafka 0.10 Example");
	}

	/**
	 * A {@link RichMapFunction} that continuously outputs the current total frequency count of a key.
	 * The current total count is keyed state managed by Flink.
	 */
	private static class RollingAdditionMapper extends RichMapFunction<KafkaEvent, KafkaEvent> {

		private static final long serialVersionUID = 1180234853172462378L;

		private transient ValueState<Integer> currentTotalCount;

		@Override
		public KafkaEvent map(KafkaEvent event) throws Exception {
			Integer totalCount = currentTotalCount.value();

			if (totalCount == null) {
				totalCount = 0;
			}
			totalCount += event.getFrequency();

			currentTotalCount.update(totalCount);

			return new KafkaEvent(event.getWord(), totalCount, event.getTimestamp1());
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			currentTotalCount = getRuntimeContext().getState(new ValueStateDescriptor<>("currentTotalCount", Integer.class));
		}
	}

	/**
	 * A custom {@link AssignerWithPeriodicWatermarks}, that simply assumes that the input stream
	 * records are strictly ascending.
	 *
	 * <p>Flink also ships some built-in convenience assigners, such as the
	 * {@link BoundedOutOfOrdernessTimestampExtractor} and {@link AscendingTimestampExtractor}
	 */
	private static class CustomWatermarkExtractor implements AssignerWithPeriodicWatermarks<KafkaEvent> {

		private static final long serialVersionUID = -742759155861320823L;

		private long currentTimestamp = Long.MIN_VALUE;

		@Override
		public long extractTimestamp(KafkaEvent event, long previousElementTimestamp) {
			// the inputs are assumed to be of format (message,timestamp)
			this.currentTimestamp = event.getTimestamp1();
			return event.getTimestamp1();
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
		}
	}
}