package com.baylor.postgresSync.sync;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Standalone Flink job for MSSQL-Debezium-Kafka-PostgreSQL pipeline This can be
 * deployed to a Flink cluster or run locally
 */
public class FlinkMSSQLToPostgresJob {

	private static final Logger logger = LoggerFactory.getLogger(FlinkMSSQLToPostgresJob.class);
	private static final ObjectMapper objectMapper = new ObjectMapper();

	public static void main(String[] args) throws Exception {
		// Parse command line arguments or use defaults
		String kafkaBootstrapServers = getProperty("kafka.bootstrap.servers", "10.27.117.113:9092");
		String kafkaTopic = getProperty("kafka.topic", "sqlserver1.sourceDBMSSQL.dbo.customers");
		String kafkaOrdersTopic = getProperty("kafka.orders.topic", "sqlserver1.sourceDBMSSQL.dbo.orders");
		String kafkaTxTopic = getProperty("kafka.tx.topic", "sqlserver1.transaction");
		String kafkaGroupId = getProperty("kafka.group.id", "flink-consumer");
		String databaseUrl = getProperty("database.url", "jdbc:postgresql://postgres:5432/targetDBPostgres");
		String databaseUsername = getProperty("database.username", "debezium");
		String databasePassword = getProperty("database.password", "dbz");
		String insertQuery = getProperty("sql.insert.query",
				"INSERT INTO customers (id, first_name, last_name, email) VALUES (?, ?, ?, ?) ON CONFLICT (id) DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email");
		String deleteQuery = getProperty("sql.delete.query", "DELETE FROM customers WHERE id = ?");
		boolean mergeMultipleTables = Boolean.parseBoolean(getProperty("merge.multiple.tables.enabled", "true"));
		boolean txMergeEnabled = Boolean.parseBoolean(getProperty("tx.merge.enabled", "true"));
		String txMergeUpsertQuery = getProperty("sql.tx.merge.upsert.query",
				"INSERT INTO customers (id, first_name, last_name, email, order_id, order_amount) VALUES (?, ?, ?, ?, ?, ?) \n"
						+ "ON CONFLICT (id) DO UPDATE SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name, email = EXCLUDED.email, order_id = EXCLUDED.order_id, order_amount = EXCLUDED.order_amount");
		String mergeInsertQuery = getProperty("sql.merge.insert.query",
				"INSERT INTO merged_events (table_name, op, row_id, payload) VALUES (?, ?, ?, CAST(? AS jsonb))");
		int batchSize = Integer.parseInt(getProperty("flink.jdbc.batch.size", "50"));
		long batchIntervalMs = Long.parseLong(getProperty("flink.jdbc.batch.interval.ms", "200"));
		int maxRetries = Integer.parseInt(getProperty("flink.jdbc.max.retries", "3"));
		long checkpointInterval = Long.parseLong(getProperty("flink.checkpoint.interval", "5000"));
		long txJoinTimeoutMs = Long.parseLong(getProperty("tx.join.timeout.ms", "5000"));

		logger.info("Starting Flink MSSQL to PostgreSQL sync job");
		logger.info("Kafka Bootstrap Servers: {}", kafkaBootstrapServers);
		logger.info("Kafka Topic: {}", kafkaTopic);
		logger.info("Database URL: {}", databaseUrl);
		logger.info("Transaction Merge Enabled: {}", txMergeEnabled);
		logger.info("Merge Multiple Tables: {}", mergeMultipleTables);

		// Create Flink execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Enable checkpointing
		env.enableCheckpointing(checkpointInterval);

		// Configure parallelism
		env.setParallelism(1); // Adjust based on your needs

		// Create Kafka source (capture topic + value)
		KafkaSource<Envelope> kafkaSource = KafkaSource.<Envelope>builder().setBootstrapServers(kafkaBootstrapServers)
				.setTopics(txMergeEnabled ? java.util.Arrays.asList(kafkaTopic, kafkaOrdersTopic, kafkaTxTopic)
						: java.util.Arrays.asList(kafkaTopic))
				.setGroupId(kafkaGroupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(new EnvelopeDeserializer()).build();

		// Create data stream from Kafka
		DataStream<Envelope> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

		// Parse once and branch for upserts and deletes (Debezium op codes: c/u/r = upsert, d = delete)
		DataStream<ObjectNode> jsonStream = stream.map(new MapFunction<Envelope, ObjectNode>() {
			private static final long serialVersionUID = 1L;
			@Override
			public ObjectNode map(Envelope record) throws Exception {
				try {
					ObjectNode json = (ObjectNode) objectMapper.readTree(record.value);
					String tableFromTopic = extractTableFromTopic(record.topic);
					json.put("__topic", record.topic);
					if (tableFromTopic != null) {
						json.put("__table", tableFromTopic);
					}
					logger.info("Received record from topic: {}, op: {}, table: {}", record.topic, 
						json.path("op").asText(), tableFromTopic);
					return json;
				} catch (Exception e) {
					logger.warn("Invalid JSON record, skipping from topic {}: {}", record.topic, record.value, e);
					return null;
				}
			}
		}).filter(node -> node != null);

		DataStream<ObjectNode> upsertStream = jsonStream
				.filter(node -> !"d".equals(node.path("op").asText()) && node.hasNonNull("after"));
		DataStream<ObjectNode> deleteStream = jsonStream
				.filter(node -> "d".equals(node.path("op").asText()) && node.hasNonNull("before"));

		// Create JDBC sink for upserts
		SinkFunction<ObjectNode> upsertSink = JdbcSink.sink(insertQuery, new CustomerUpsertStatementBuilder(),
				JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(batchIntervalMs)
						.withMaxRetries(maxRetries).build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl)
						.withDriverName("org.postgresql.Driver").withUsername(databaseUsername)
						.withPassword(databasePassword).build());

		// Create JDBC sink for deletes
		SinkFunction<ObjectNode> deleteSink = JdbcSink.sink(deleteQuery, new CustomerDeleteStatementBuilder(),
				JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(batchIntervalMs)
						.withMaxRetries(maxRetries).build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl)
						.withDriverName("org.postgresql.Driver").withUsername(databaseUsername)
						.withPassword(databasePassword).build());

        if (txMergeEnabled) {
			// Transaction-aware merge: join within tx and write combined record
            org.apache.flink.streaming.api.datastream.DataStream<TxEvent> txEvents = jsonStream
                    .map(new org.apache.flink.api.common.functions.MapFunction<ObjectNode, TxEvent>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public TxEvent map(ObjectNode value) {
                            return toTxEvent(value);
                        }
                    })
					.filter(evt -> evt != null && evt.txId != null && !evt.txId.isEmpty());
			org.apache.flink.streaming.api.datastream.DataStream<CombinedRecord> combined = txEvents
					.keyBy(evt -> evt.txId).process(new TxJoinProcess(txJoinTimeoutMs)).name("TxJoinProcess");

			SinkFunction<CombinedRecord> txMergeSink = JdbcSink.sink(txMergeUpsertQuery, new TxMergeStatementBuilder(),
					JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(batchIntervalMs)
							.withMaxRetries(maxRetries).build(),
					new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl)
							.withDriverName("org.postgresql.Driver").withUsername(databaseUsername)
							.withPassword(databasePassword).build());

			combined.addSink(txMergeSink).name("PostgreSQL Tx Merge Sink");
		} else if (mergeMultipleTables) {
			// Single merged-events sink
			SinkFunction<ObjectNode> mergeSink = JdbcSink.sink(mergeInsertQuery, new MergeStatementBuilder(),
					JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(batchIntervalMs)
							.withMaxRetries(maxRetries).build(),
					new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl)
							.withDriverName("org.postgresql.Driver").withUsername(databaseUsername)
							.withPassword(databasePassword).build());

			jsonStream.addSink(mergeSink).name("PostgreSQL Merge Sink");
		} else {
			// Original behavior: upsert + delete
			upsertStream.addSink(upsertSink).name("PostgreSQL Upsert Sink");
			deleteStream.addSink(deleteSink).name("PostgreSQL Delete Sink");
		}

		// Execute the job
		env.execute("MSSQL-Debezium-Kafka-PostgreSQL Sync Job");
	}

	/**
	 * JDBC statement builder for upsert (uses Debezium "after")
	 */
	public static class CustomerUpsertStatementBuilder implements JdbcStatementBuilder<ObjectNode> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(CustomerUpsertStatementBuilder.class);

		@Override
		public void accept(PreparedStatement ps, ObjectNode record) throws SQLException {
			try {
				JsonNode after = record.get("after");

				if (after == null) {
					logger.warn("No 'after' field found in record");
					return;
				}

				// Extract fields with null checks
				int id = after.has("id") ? after.get("id").asInt() : 0;
				String firstName = after.has("first_name") ? after.get("first_name").asText() : "";
				String lastName = after.has("last_name") ? after.get("last_name").asText() : "";
				String email = after.has("email") ? after.get("email").asText() : "";

				// Set parameters
				ps.setInt(1, id);
				ps.setString(2, firstName);
				ps.setString(3, lastName);
				ps.setString(4, email);

				logger.debug("Upsert prepared for id: {}, name: {} {}", id, firstName, lastName);

			} catch (Exception e) {
				logger.error("Error processing upsert record", e);
				throw new SQLException("Failed to process upsert record", e);
			}
		}
	}

	/**
	 * JDBC statement builder for delete (uses Debezium "before")
	 */
	public static class CustomerDeleteStatementBuilder implements JdbcStatementBuilder<ObjectNode> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(CustomerDeleteStatementBuilder.class);

		@Override
		public void accept(PreparedStatement ps, ObjectNode record) throws SQLException {
			try {
				JsonNode before = record.get("before");
				if (before == null) {
					logger.warn("No 'before' field found for delete record");
					return;
				}

				int id = before.has("id") ? before.get("id").asInt() : 0;
				ps.setInt(1, id);
				logger.debug("Delete prepared for id: {}", id);
			} catch (Exception e) {
				logger.error("Error processing delete record", e);
				throw new SQLException("Failed to process delete record", e);
			}
		}
	}

	/**
	 * JDBC statement builder for merging multiple tables into a single target
	 * table. Expects SQL: INSERT INTO merged_events(table_name, op, row_id,
	 * payload) VALUES(?, ?, ?, CAST(? AS jsonb))
	 */
	public static class MergeStatementBuilder implements JdbcStatementBuilder<ObjectNode> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(MergeStatementBuilder.class);

		@Override
		public void accept(PreparedStatement ps, ObjectNode record) throws SQLException {
			try {
				String op = record.path("op").asText("");
				String tableName = record.has("__table") ? record.path("__table").asText("")
						: record.path("source").path("table").asText("");
				JsonNode keyNode = "d".equals(op) ? record.path("before") : record.path("after");
				int id = keyNode.has("id") ? keyNode.get("id").asInt() : 0;
				String payload = keyNode.toString();

				ps.setString(1, tableName);
				ps.setString(2, op);
				ps.setInt(3, id);
				ps.setString(4, payload);
				logger.debug("Merge event table={}, op={}, id={}", tableName, op, id);
			} catch (Exception e) {
				logger.error("Error building merge statement", e);
				throw new SQLException("Failed to build merge statement", e);
			}
		}
	}

	/** Envelope with topic and value */
	public static class Envelope {
		public final String topic;
		public final String value;

		public Envelope(String topic, String value) {
			this.topic = topic;
			this.value = value;
		}
	}

	/** Kafka deserializer producing Envelope(topic,value) */
	public static class EnvelopeDeserializer implements KafkaRecordDeserializationSchema<Envelope> {
		private static final long serialVersionUID = 1L;
		private transient StringDeserializer valueDeser;

		@Override
		public void open(org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext context) throws Exception {
			valueDeser = new StringDeserializer();
			valueDeser.configure(new java.util.HashMap<>(), false);
		}

		@Override
		public void deserialize(ConsumerRecord<byte[], byte[]> record, org.apache.flink.util.Collector<Envelope> out) {
			String value = valueDeser.deserialize(record.topic(), record.value());
			out.collect(new Envelope(record.topic(), value));
		}

		@Override
		public org.apache.flink.api.common.typeinfo.TypeInformation<Envelope> getProducedType() {
			return org.apache.flink.api.java.typeutils.TypeExtractor.getForClass(Envelope.class);
		}
	}

	private static String extractTableFromTopic(String topic) {
		// Debezium default topic: <server>.<db>.<schema>.<table>
		try {
			String[] parts = topic.split("\\.");
			return parts.length > 0 ? parts[parts.length - 1] : null;
		} catch (Exception e) {
			return null;
		}
	}

	// ---- Transaction-aware merge implementation ----

	private static TxEvent toTxEvent(ObjectNode node) {
		String topic = node.path("__topic").asText("");
		String op = node.path("op").asText("");
		String txId;
		if (topic.endsWith(".transaction")) {
			txId = node.path("id").asText("");
			String status = node.path("status").asText("");
			logger.info("Transaction event: txId={}, status={}", txId, status);
			return new TxEvent(txId, status.equalsIgnoreCase("END") ? EventType.TX_END : EventType.TX_OTHER, null,
					null);
		} else {
			txId = node.path("transaction").path("id").asText("");
			if (topic.endsWith(".customers") && node.hasNonNull("after")) {
				logger.info("Customer event: txId={}, op={}", txId, op);
				return new TxEvent(txId, EventType.CUSTOMER, (ObjectNode) node.get("after"), null);
			}
			if (topic.endsWith(".orders") && ("c".equals(op) || "u".equals(op)) && node.hasNonNull("after")) {
				ObjectNode order = (ObjectNode) node.get("after");
				Integer orderId = order.has("id") ? order.get("id").asInt() : null;
				Long amount = order.has("amount") ? order.get("amount").asLong() : null;
				logger.info("Order event: txId={}, op={}, orderId={}, amount={}", txId, op, orderId, amount);
				return new TxEvent(txId, EventType.ORDER, null, new OrderInfo(orderId, amount));
			}
		}
		logger.debug("No valid transaction event found for topic: {}, op: {}", topic, op);
		return null;
	}

	enum EventType {
		CUSTOMER, ORDER, TX_END, TX_OTHER
	}

	static class OrderInfo {
		final Integer orderId;
		final Long amount;

		OrderInfo(Integer orderId, Long amount) {
			this.orderId = orderId;
			this.amount = amount;
		}
	}

	static class TxEvent {
		final String txId;
		final EventType type;
		final ObjectNode customerAfter;
		final OrderInfo order;

		TxEvent(String txId, EventType type, ObjectNode customerAfter, OrderInfo order) {
			this.txId = txId;
			this.type = type;
			this.customerAfter = customerAfter;
			this.order = order;
		}
	}

	static class CombinedRecord {
		final int customerId;
		final String firstName;
		final String lastName;
		final String email;
		final Integer orderId;
		final Long amount;

		CombinedRecord(int customerId, String firstName, String lastName, String email, Integer orderId, Long amount) {
			this.customerId = customerId;
			this.firstName = firstName;
			this.lastName = lastName;
			this.email = email;
			this.orderId = orderId;
			this.amount = amount;
		}
	}

static class TxJoinProcess extends KeyedProcessFunction<String, TxEvent, CombinedRecord> {
    private final long timeoutMs;
    private transient ValueState<ObjectNode> customerState;
    private transient ValueState<OrderInfo> lastOrderState;
    private transient ValueState<Long> timerState;

    TxJoinProcess(long timeoutMs) {
        this.timeoutMs = timeoutMs;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) {
        customerState = getRuntimeContext().getState(new ValueStateDescriptor<>("customer", ObjectNode.class));
        lastOrderState = getRuntimeContext().getState(new ValueStateDescriptor<>("lastOrder", OrderInfo.class));
        timerState = getRuntimeContext().getState(new ValueStateDescriptor<>("timerTs", Long.class));
    }

		@Override
		public void processElement(TxEvent value, Context ctx, Collector<CombinedRecord> out) throws Exception {
			switch (value.type) {
            case CUSTOMER:
                customerState.update(value.customerAfter);
                logger.info("Stored customer state for txId: {}", value.txId);
                registerOrRefreshTimer(ctx);
                break;
            case ORDER:
                lastOrderState.update(value.order);
                logger.info("Stored order state for txId: {}", value.txId);
                registerOrRefreshTimer(ctx);
                break;
            case TX_END:
                emitIfCompleteAndClear(out);
                clearTimer(ctx);
                break;
			default:
			}
		}

    private void emitIfCompleteAndClear(Collector<CombinedRecord> out) throws Exception {
        ObjectNode cust = customerState.value();
        OrderInfo ord = lastOrderState.value();
        if (cust != null && ord != null) {
            int id = cust.has("id") ? cust.get("id").asInt() : 0;
            String first = cust.has("first_name") ? cust.get("first_name").asText() : "";
            String last = cust.has("last_name") ? cust.get("last_name").asText() : "";
            String email = cust.has("email") ? cust.get("email").asText() : "";
            out.collect(new CombinedRecord(id, first, last, email, ord.orderId, ord.amount));
        } else {
            logger.warn("Emit attempted but missing data - customer: {}, order: {}", cust != null, ord != null);
        }
        customerState.clear();
        lastOrderState.clear();
    }

    private void registerOrRefreshTimer(Context ctx) throws Exception {
        long now = ctx.timerService().currentProcessingTime();
        Long existing = timerState.value();
        if (existing != null) {
            ctx.timerService().deleteProcessingTimeTimer(existing);
        }
        long next = now + timeoutMs;
        ctx.timerService().registerProcessingTimeTimer(next);
        timerState.update(next);
    }

    private void clearTimer(Context ctx) throws Exception {
        Long existing = timerState.value();
        if (existing != null) {
            ctx.timerService().deleteProcessingTimeTimer(existing);
            timerState.clear();
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<CombinedRecord> out) throws Exception {
        logger.info("Timer fired for txId: {}", ctx.getCurrentKey());
        emitIfCompleteAndClear(out);
        timerState.clear();
    }
	}

	public static class TxMergeStatementBuilder implements JdbcStatementBuilder<CombinedRecord> {
		private static final long serialVersionUID = 1L;

		@Override
		public void accept(PreparedStatement ps, CombinedRecord r) throws SQLException {
			logger.info("Executing SQL insert for customer: id={}, name={} {}, email={}, orderId={}, amount={}", 
				r.customerId, r.firstName, r.lastName, r.email, r.orderId, r.amount);
			ps.setInt(1, r.customerId);
			ps.setString(2, r.firstName);
			ps.setString(3, r.lastName);
			ps.setString(4, r.email);
			if (r.orderId != null)
				ps.setInt(5, r.orderId);
			else
				ps.setNull(5, java.sql.Types.INTEGER);
			if (r.amount != null)
				ps.setLong(6, r.amount);
			else
				ps.setNull(6, java.sql.Types.BIGINT);
		}
	}

	/**
	 * Get property from system properties or environment variables
	 */
	private static String getProperty(String key, String defaultValue) {
		// Try system property first
		String value = System.getProperty(key);
		if (value != null) {
			return value;
		}

		// Try environment variable
		value = System.getenv(key.toUpperCase().replace(".", "_"));
		if (value != null) {
			return value;
		}

		return defaultValue;
	}
}
