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

import net.sf.json.JSONObject;

/**
 * The event type used in the {@link Kafka010Example}.
 *
 * <p>This is a Java POJO, which Flink recognizes and will allow "by-name" field referencing
 * when keying a {@link org.apache.flink.streaming.api.datastream.DataStream} of such a type.
 * For a demonstration of this, see the code in {@link Kafka010Example}.
 */
public class KafkaEvent {

	private String word;
	private int frequency;
	private long timestamp1;

	public KafkaEvent() {}

	public KafkaEvent(String word, int frequency, long timestamp1) {
		this.word = word;
		this.frequency = frequency;
		this.timestamp1 = timestamp1;
	}

	public String getWord() {
		return word;
	}

	public void setWord(String word) {
		this.word = word;
	}

	public int getFrequency() {
		return frequency;
	}

	public void setFrequency(int frequency) {
		this.frequency = frequency;
	}

	public long getTimestamp1() {
		return timestamp1;
	}

	public void setTimestamp1(long timestamp1) {
		this.timestamp1 = timestamp1;
	}

	public static KafkaEvent fromString(String eventStr) {
		JSONObject jsonObject = JSONObject.fromObject(eventStr);
		return new KafkaEvent(jsonObject.getString("word"), jsonObject.getInt("frequency"), jsonObject.getLong("timestamp"));
	}

	@Override
	public String toString() {
		return word + "," + frequency + "," + timestamp1;
	}
}