/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.statsd;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Largely based on the StatsDReporter class by ReadyTalk.
 *
 * <p>https://github.com/ReadyTalk/metrics-statsd/blob/master/metrics3-statsd/src/main/java/com/readytalk/metrics/StatsDReporter.java
 *
 * <p>Ported since it was not present in maven central.
 */
@PublicEvolving
public class StatsDReporter extends AbstractReporter implements Scheduled {

	private static final Logger LOG = LoggerFactory.getLogger(StatsDReporter.class);

	private static final String ARG_HOST = "host";
	private static final String ARG_PORT = "port";
	private static final String ARG_DOGSTATSD = "dogstatsd";
	private static final String ARG_SHORTIDS = "shortids";
	//private static final String HOST_TAG = "<host>";

	private boolean closed = false;

	private DatagramSocket socket;
	private InetSocketAddress address;

	private boolean dogstatsdMode;
	private boolean shortIds;

	private final Map<Metric, String> tagTable = new ConcurrentHashMap<>();

	@Override
	public void open(MetricConfig config) {
		String host = config.getString(ARG_HOST, null);
		int port = config.getInteger(ARG_PORT, -1);

		dogstatsdMode = config.getBoolean(ARG_DOGSTATSD, false);
		shortIds = config.getBoolean(ARG_SHORTIDS, false);

		if (host == null || host.length() == 0 || port < 1) {
			throw new IllegalArgumentException("Invalid host/port configuration. Host: " + host + " Port: " + port);
		}

		this.address = new InetSocketAddress(host, port);

		try {
			this.socket = new DatagramSocket(0);
		} catch (SocketException e) {
			throw new RuntimeException("Could not create datagram socket. ", e);
		}
		log.info("Configured StatsDReporter with config: {}", config);
	}

	@Override
	public void close() {
		closed = true;
		if (socket != null && !socket.isClosed()) {
			socket.close();
		}
	}

	// ------------------------------------------------------------------------
	/**
	 * Removes leading and trailing angle brackets.
	 */
	private String stripBrackets(String str) {
		return str.substring(1, str.length() - 1);
	}

	@Override
	public void notifyOfAddedMetric(Metric metric, String metricName, MetricGroup group) {
		if (dogstatsdMode) {
			// memoize dogstatsd tag section: "|#tag:val,tag:val,tag:val"
			StringBuilder statsdTagLine = new StringBuilder();
			Map<String, String> orderedTags = new TreeMap<>(group.getAllVariables());
			for (Map.Entry<String, String> entry: orderedTags.entrySet()) {
				String k = stripBrackets(entry.getKey());
				String v = filterCharacters(entry.getValue());
				statsdTagLine.append(",").append(k).append(":").append(v);
			}
			if (statsdTagLine.length() > 0) {
				// remove first comma, prefix with "|#"
				tagTable.put(metric, "|#" + statsdTagLine.substring(1));

				String name = metric.getClass().getSimpleName();
				if (name.length() == 0) {
					name = metric.toString();
				}
			}
		}
		super.notifyOfAddedMetric(metric, metricName, group);
	}

	@Override
	public void notifyOfRemovedMetric(Metric metric, String metricName, MetricGroup group) {
		if (dogstatsdMode) {
			tagTable.remove(metric);
		}
		super.notifyOfRemovedMetric(metric, metricName, group);
	}

	@Override
	public void report() {
		// instead of locking here, we tolerate exceptions
		// we do this to prevent holding the lock for very long and blocking
		// operator creation and shutdown
		try {
			for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
				if (closed) {
					return;
				}
				reportGauge(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Counter, String> entry : counters.entrySet()) {
				if (closed) {
					return;
				}
				reportCounter(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Histogram, String> entry : histograms.entrySet()) {
				reportHistogram(entry.getValue(), entry.getKey());
			}

			for (Map.Entry<Meter, String> entry : meters.entrySet()) {
				reportMeter(entry.getValue(), entry.getKey());
			}
		}
		catch (ConcurrentModificationException | NoSuchElementException e) {
			// ignore - may happen when metrics are concurrently added or removed
			// report next time
		}
	}

	// ------------------------------------------------------------------------

	private void reportCounter(final String name, final Counter counter) {
		send(name, String.valueOf(counter.getCount()), tagTable.get(counter));
	}

	private void reportGauge(final String name, final Gauge<?> gauge) {
		Object value = gauge.getValue();
		if (value == null) {
			return;
		}
		String tags = tagTable.get(gauge);
		if (value instanceof Map) {
			// LatencyGauge is a Map<String, HashMap<String,Double>>
			for (Object m: ((Map<?, ?>) value).values()) {
				if (m instanceof Map) {
					for (Map.Entry<?, ?> entry: ((Map<?, ?>) m).entrySet()) {
						String k = String.valueOf(entry.getKey());
						String v = String.valueOf(entry.getValue());
						send(prefix(name, k), v, tags);
					}
				}
			}
		} else {
			send(name, value.toString(), tags);
		}
	}

	private void reportHistogram(final String name, final Histogram histogram) {
		if (histogram != null) {

			HistogramStatistics statistics = histogram.getStatistics();
			String tags = tagTable.get(histogram);

			if (statistics != null) {
				send(prefix(name, "count"), String.valueOf(histogram.getCount()), tags);
				send(prefix(name, "max"), String.valueOf(statistics.getMax()), tags);
				send(prefix(name, "min"), String.valueOf(statistics.getMin()), tags);
				send(prefix(name, "mean"), String.valueOf(statistics.getMean()), tags);
				send(prefix(name, "stddev"), String.valueOf(statistics.getStdDev()), tags);
				send(prefix(name, "p50"), String.valueOf(statistics.getQuantile(0.5)), tags);
				send(prefix(name, "p75"), String.valueOf(statistics.getQuantile(0.75)), tags);
				send(prefix(name, "p95"), String.valueOf(statistics.getQuantile(0.95)), tags);
				send(prefix(name, "p98"), String.valueOf(statistics.getQuantile(0.98)), tags);
				send(prefix(name, "p99"), String.valueOf(statistics.getQuantile(0.99)), tags);
				send(prefix(name, "p999"), String.valueOf(statistics.getQuantile(0.999)), tags);
			}
		}
	}

	private void reportMeter(final String name, final Meter meter) {
		if (meter != null) {
			String tags = tagTable.get(meter);
			send(prefix(name, "rate"), String.valueOf(meter.getRate()), tags);
			send(prefix(name, "count"), String.valueOf(meter.getCount()), tags);
		}
	}

	private String prefix(String ... names) {
		if (names.length > 0) {
			StringBuilder stringBuilder = new StringBuilder(names[0]);

			for (int i = 1; i < names.length; i++) {
				stringBuilder.append('.').append(names[i]);
			}

			return stringBuilder.toString();
		} else {
			return "";
		}
	}

	private String buildStatsdLine(final String name, final String value, final String tags) {
		Double number;
		try {
			number = Double.parseDouble(value);
		}
		catch (NumberFormatException e) {
			// quietly skip values like "n/a"
			return "";
		}
		if (number >= 0.) {
			return String.format("%s:%s|g%s", name, value, tags != null ? tags : "");
		} else {
			// quietly skip "unknowns" like lowWaterMark:-9223372036854775808, or JVM.Memory.NonHeap.Max:-1, or NaN
			return "";
		}
	}

	private void send(final String name, final String value, final String tags) {
		String formatted = buildStatsdLine(name, value, tags);
		if (formatted.length() > 0) {
			try {
				byte[] data = formatted.getBytes(StandardCharsets.UTF_8);
				socket.send(new DatagramPacket(data, data.length, this.address));
			}
			catch (IOException e) {
				LOG.error("unable to send packet to statsd at '{}:{}'", address.getHostName(), address.getPort());
			}
		}
	}

	/**
	* DataDog guidance on statsd names: start with letter, uses ascii alphanumerics and underscore, separated by periods.
	* Collapse invalid character(s) into an underscore. Skip invalid prefix and suffix.
	*/

	private Boolean isValidStatsdChar(char c) {
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || (c == '_');
	}

	private String filterNCharacters(String input, Integer limit) {
		char[] chars = null;
		final int strLen = input.length();
		int pos = 0;
		Boolean insertFiller = false;

		for (int i = 0; i < strLen && pos < limit; i++) {
			final char c = input.charAt(i);
			if (isValidStatsdChar(c)) {
				if (chars != null) {
					// skip invalid suffix, only fill if followed by valid character
					if (insertFiller) {
						chars[pos++] = '_';
						insertFiller = false;
					}
					chars[pos] = c;
				}
				pos++;
			} else {
				if (chars == null) {
					chars = input.toCharArray();
				}
				// skip invalid prefix, until pos > 0
				if (pos > 0) {
					// collapse sequence of invalid char into one filler
					insertFiller = true;
				}
			}
		}

		if (chars == null) {
			if (strLen > limit) {
				return input.substring(0, limit);
			} else {
				return input; // happy path, input is entirely valid and under the limit
			}
		} else {
			return new String(chars, 0, pos);
		}
	}

	@Override
	public String filterCharacters(String input) {
		if (input.length() < 50) {
			Integer limit = Integer.MAX_VALUE;
			if (shortIds && input.length() == 32 && !input.contains("_")) {
				limit = 8;
			}
			return filterNCharacters(input, limit);
		}
		// remove hash() references to stabilize the identifier
		String stableName = input.replaceAll("@[0-9a-f]+", "");
		return filterNCharacters(stableName, 10) + "_" + Integer.toHexString(stableName.hashCode());
	}
}
