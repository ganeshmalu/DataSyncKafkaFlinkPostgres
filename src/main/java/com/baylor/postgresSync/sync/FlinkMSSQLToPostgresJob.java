package com.baylor.postgresSync.sync;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		String kafkaSamplesTopic = getProperty("kafka.Samples.topic", "sqlserver.MGL.dbo.Samples");
		String kafkaSamplesAdditionalInfoTopic = getProperty("kafka.SamplesAdditionalInfo.topic",
				"sqlserver.MGL.dbo.SamplesAdditionalInfo");
		String kafkaGroupId = getProperty("kafka.group.id", "flink-consumer");
		String databaseUrl = getProperty("database.url", "jdbc:postgresql://10.27.117.24:5432/targetDBPostgres");
		String databaseUsername = getProperty("database.username", "svc_pgsql_admin");
		String databasePassword = getProperty("database.password", "BGpgsql@DMIN");
		String sessionTimeoutMs = getProperty("session.timeout.ms", "5000");
		String upsertQuery = getProperty("sql.upsert.query",
				"INSERT INTO SampleDetails (patientFirstName, patientLastName, sampleId, SamplesAdditionalInfoId, AdditionalInformation, updated_at) VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP) \n"
						+ "ON CONFLICT (sampleId) DO UPDATE SET \n"
						+ "  patientFirstName = CASE WHEN EXCLUDED.patientFirstName IS NOT NULL THEN EXCLUDED.patientFirstName ELSE SampleDetails.patientFirstName END, \n"
						+ "  patientLastName = CASE WHEN EXCLUDED.patientLastName IS NOT NULL THEN EXCLUDED.patientLastName ELSE SampleDetails.patientLastName END, \n"
						+ "  SamplesAdditionalInfoId = CASE WHEN EXCLUDED.SamplesAdditionalInfoId IS NOT NULL THEN EXCLUDED.SamplesAdditionalInfoId ELSE SampleDetails.SamplesAdditionalInfoId END, \n"
						+ "  AdditionalInformation = CASE WHEN EXCLUDED.AdditionalInformation IS NOT NULL THEN EXCLUDED.AdditionalInformation ELSE SampleDetails.AdditionalInformation END, \n"
						+ "  updated_at = CURRENT_TIMESTAMP");
		int batchSize = Integer.parseInt(getProperty("flink.jdbc.batch.size", "50"));
		long batchIntervalMs = Long.parseLong(getProperty("flink.jdbc.batch.interval.ms", "200"));
		int maxRetries = Integer.parseInt(getProperty("flink.jdbc.max.retries", "3"));
		long checkpointInterval = Long.parseLong(getProperty("flink.checkpoint.interval", "10000"));

		logger.info("Starting Flink MSSQL to PostgreSQL sync job");
		logger.info("Kafka Bootstrap Servers: {}", kafkaBootstrapServers);
		logger.info("Kafka Samples Topic: {}", kafkaSamplesTopic);
		logger.info("Kafka SamplesAdditionalInfo Topic: {}", kafkaSamplesAdditionalInfoTopic);
		logger.info("Database URL: {}", databaseUrl);
		logger.info("Session Timeout: {}ms", sessionTimeoutMs);

		// Create Flink execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Enable checkpointing
		env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
		
		// Configure checkpoint storage (local or distributed)
		env.setStateBackend(new HashMapStateBackend());
		env.getCheckpointConfig().setCheckpointStorage("file:///opt/flink/usrlib");

		// Configure parallelism
		env.setParallelism(1); // Adjust based on your needs

		// Create Kafka source (capture topic + value)
		KafkaSource<Envelope> kafkaSource = KafkaSource.<Envelope>builder().setBootstrapServers(kafkaBootstrapServers)
				.setTopics(java.util.Arrays.asList(kafkaSamplesTopic, kafkaSamplesAdditionalInfoTopic))
				.setGroupId(kafkaGroupId).setStartingOffsets(OffsetsInitializer.committedOffsets())
				.setDeserializer(new EnvelopeDeserializer()).build();

		// Create data stream from Kafka
		DataStream<Envelope> stream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

		// Parse and extract transaction events
		DataStream<TxEvent> txEvents = stream.map(new MapFunction<Envelope, TxEvent>() {
			private static final long serialVersionUID = 1L;

			@Override
			public TxEvent map(Envelope record) throws Exception {
				try {
					ObjectNode json = (ObjectNode) objectMapper.readTree(record.value);
					String tableFromTopic = extractTableFromTopic(record.topic);
					json.put("__topic", record.topic);
					if (tableFromTopic != null) {
						json.put("__table", tableFromTopic);
					}
					logger.info("Received record from topic: {}, op: {}, table: {}", record.topic,
							json.path("op").asText(), tableFromTopic);
					return toTxEvent(json);
				} catch (Exception e) {
					logger.warn("Invalid JSON record, skipping from topic {}: {}", record.topic, record.value, e);
					return null;
				}
			}
		}).filter(evt -> evt != null && evt.txId != null && !evt.txId.isEmpty());

		// Session window by transaction ID with timeout
		DataStream<SessionResult> sessionResults = txEvents.keyBy(evt -> evt.txId)
				.window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(Long.parseLong(sessionTimeoutMs))))
				.process(new SessionWindowProcessor()).name("SessionWindowProcessor");

		// Upserts stream
		DataStream<SampleDetailsRecord> upsertStream = sessionResults
				.filter(result -> result.type == SessionResultType.UPSERT).map(result -> result.record);

		// Deletes stream
		DataStream<SampleDetailsRecord> deleteStream = sessionResults
				.filter(result -> result.type == SessionResultType.DELETE).map(result -> result.record);

		// Create JDBC upsert sink
		SinkFunction<SampleDetailsRecord> upsertSink = JdbcSink.sink(upsertQuery, new SessionMergeStatementBuilder(),
				JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(batchIntervalMs)
						.withMaxRetries(maxRetries).build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl)
						.withDriverName("org.postgresql.Driver").withUsername(databaseUsername)
						.withPassword(databasePassword).build());

		// Create JDBC delete sink
		String deleteQuery = "DELETE FROM SampleDetails WHERE sampleId = ?";
		SinkFunction<SampleDetailsRecord> deleteSink = JdbcSink.sink(deleteQuery, new SessionDeleteStatementBuilder(),
				JdbcExecutionOptions.builder().withBatchSize(batchSize).withBatchIntervalMs(batchIntervalMs)
						.withMaxRetries(maxRetries).build(),
				new JdbcConnectionOptions.JdbcConnectionOptionsBuilder().withUrl(databaseUrl)
						.withDriverName("org.postgresql.Driver").withUsername(databaseUsername)
						.withPassword(databasePassword).build());

		upsertStream.addSink(upsertSink).name("PostgreSQL Upsert Sink");
		deleteStream.addSink(deleteSink).name("PostgreSQL Delete Sink");

		// Execute the job
		env.execute("MSSQL-Debezium-Kafka-PostgreSQL Sync Job");
	}

	/**
	 * Session window processor that combines customer and order events
	 */
	public static class SessionWindowProcessor
			extends ProcessWindowFunction<TxEvent, SessionResult, String, TimeWindow> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(SessionWindowProcessor.class);

		@Override
		public void process(String txId, Context context, Iterable<TxEvent> events, Collector<SessionResult> out)
				throws Exception {
			ObjectNode samples = null;
			ObjectNode samplesDelete = null;
			AdditionalInfo addInfo = null;
			AdditionalInfo addInfoDelete = null;
			int eventCount = 0;

			for (TxEvent event : events) {
				eventCount++;
				switch (event.type) {
                case SAMPLES:
                    samples = event.samplesAfter;
					logger.info("Session window - stored Samples for txId: {}", txId);
					break;
				case SAMPLESADDITIONALINFO:
					addInfo = event.addInfo;
					logger.info("Session window - stored SamplesAdditionalInfo for txId: {}", txId);
					break;
				case SAMPLES_DELETE:
					samplesDelete = event.samplesAfter; // contains 'before' data for delete
					logger.info("Session window - stored Samples delete for txId: {}", txId);
					break;
				case SAMPLESADDITIONALINFO_DELETE:
					addInfoDelete = event.addInfo;
					logger.info("Session window - stored SamplesAdditionalInfo delete for txId: {}", txId);
					break;
				default:
					// Ignore transaction metadata events
					break;
				}
			}

			// Handle upsert scenarios:
			// 1. Samples + optional SamplesAdditionalInfo
			// 2. SamplesAdditionalInfo only (update existing SampleDetails)
			if (samples != null) {
                // Extra logs to diagnose mapping issues
                try {
                    StringBuilder keys = new StringBuilder();
                    java.util.Iterator<String> it = samples.fieldNames();
                    while (it.hasNext()) { if (keys.length() > 0) keys.append(','); keys.append(it.next()); }
                    logger.info("Samples keys: {}", keys.toString());
                    logger.info("Samples id candidates -> id: {}, Id: {}, SampleId: {}", samples.path("id"), samples.path("Id"), samples.path("SampleId"));
                } catch (Exception logEx) {
                    logger.warn("Failed to log Samples keys: {}", logEx.getMessage());
                }

                String patientFirstName = samples.has("PatientFirstName") && !samples.get("PatientFirstName").isNull()
                        ? samples.get("PatientFirstName").asText("")
                        : samples.path("patientFirstName").asText("");
                String patientLastName = samples.has("PatientLastName") && !samples.get("PatientLastName").isNull()
                        ? samples.get("PatientLastName").asText("")
                        : samples.path("patientLastName").asText("");

                Long sampleId;
                if (samples.has("id") && !samples.get("id").isNull()) {
                    sampleId = samples.get("id").isNumber() ? samples.get("id").asLong() : parseLongSafe(samples.get("id").asText(), 0L);
                } else if (samples.has("Id") && !samples.get("Id").isNull()) {
                    sampleId = samples.get("Id").isNumber() ? samples.get("Id").asLong() : parseLongSafe(samples.get("Id").asText(), 0L);
                } else if (samples.has("SampleId") && !samples.get("SampleId").isNull()) {
                    sampleId = samples.get("SampleId").isNumber() ? samples.get("SampleId").asLong() : parseLongSafe(samples.get("SampleId").asText(), 0L);
                } else if (addInfo != null && addInfo.sampleIdRef != null) {
                    sampleId = addInfo.sampleIdRef;
                } else {
                    sampleId = 0L;
                }
				Long samplesAdditionalInfoId = (addInfo != null && addInfo.additionalInfoId != null)
						? addInfo.additionalInfoId
						: null;
				String additionalInformation = (addInfo != null && addInfo.additionalInformation != null)
						? addInfo.additionalInformation
						: null;

				logger.info(
						"Session window closed - emitting SampleDetails: txId={}, patientFirstName={}, patientLastName={}, sampleId={}, SamplesAdditionalInfoId={}, AdditionalInformation={}",
						txId, patientFirstName, patientLastName, sampleId, samplesAdditionalInfoId,
						additionalInformation);

				out.collect(new SessionResult(SessionResultType.UPSERT, new SampleDetailsRecord(patientFirstName,
						patientLastName, sampleId, samplesAdditionalInfoId, additionalInformation), null));
			} else if (addInfo != null && addInfo.sampleIdRef != null) {
				// Scenario 2: SamplesAdditionalInfo only - update existing SampleDetails
				// Use sampleId from SamplesAdditionalInfo.SampleId, nulls for patient names
				logger.info("SamplesAdditionalInfo-only update: txId={}, sampleId={}, addInfoId={}", 
					txId, addInfo.sampleIdRef, addInfo.additionalInfoId);
				
				out.collect(new SessionResult(SessionResultType.UPSERT, new SampleDetailsRecord(
					null, // patientFirstName - not available from SamplesAdditionalInfo
					null, // patientLastName - not available from SamplesAdditionalInfo  
					addInfo.sampleIdRef, // sampleId from SamplesAdditionalInfo.SampleId
					addInfo.additionalInfoId, // SamplesAdditionalInfoId
					addInfo.additionalInformation), null));
			} else {
				logger.warn("Session window closed but missing both Samples and SamplesAdditionalInfo data - txId={}, events={}", txId, eventCount);
			}

			// Handle delete scenarios:
			// 3. Samples delete - remove entire SampleDetails record
			// 4. SamplesAdditionalInfo delete - clear additional info fields
			if (samplesDelete != null) {
				// Extract sampleId from deleted Samples record
				Long sampleId;
				if (samplesDelete.has("id") && !samplesDelete.get("id").isNull()) {
					sampleId = samplesDelete.get("id").isNumber() ? samplesDelete.get("id").asLong() : parseLongSafe(samplesDelete.get("id").asText(), 0L);
				} else if (samplesDelete.has("Id") && !samplesDelete.get("Id").isNull()) {
					sampleId = samplesDelete.get("Id").isNumber() ? samplesDelete.get("Id").asLong() : parseLongSafe(samplesDelete.get("Id").asText(), 0L);
				} else {
					sampleId = 0L;
				}
				
				if (sampleId > 0) {
					logger.info("Samples delete - removing SampleDetails record: txId={}, sampleId={}", txId, sampleId);
					out.collect(new SessionResult(SessionResultType.DELETE, new SampleDetailsRecord(null, null, sampleId, null, null), null));
				}
			} else if (addInfoDelete != null && addInfoDelete.sampleIdRef != null) {
				// SamplesAdditionalInfo delete - clear additional info fields but keep patient data
				logger.info("SamplesAdditionalInfo delete - clearing additional info: txId={}, sampleId={}", txId, addInfoDelete.sampleIdRef);
				out.collect(new SessionResult(SessionResultType.UPSERT, new SampleDetailsRecord(
					null, // patientFirstName - keep existing
					null, // patientLastName - keep existing  
					addInfoDelete.sampleIdRef, // sampleId
					null, // SamplesAdditionalInfoId - clear
					null), null)); // AdditionalInformation - clear
			}
		}
	}

	/**
	 * JDBC statement builder for session merge results
	 */
	public static class SessionMergeStatementBuilder implements JdbcStatementBuilder<SampleDetailsRecord> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(SessionMergeStatementBuilder.class);

		@Override
		public void accept(PreparedStatement ps, SampleDetailsRecord r) throws SQLException {
			logger.info(
					"Executing SQL upsert for sample: sampleId={}, patientFirstName={}, patientLastName={}, addInfoId={}",
					r.sampleId, r.patientFirstName, r.patientLastName, r.samplesAdditionalInfoId);
			
			// Always set values (SQL handles null checking with CASE statements)
			if (r.patientFirstName != null)
				ps.setString(1, r.patientFirstName);
			else
				ps.setNull(1, java.sql.Types.VARCHAR);
				
			if (r.patientLastName != null)
				ps.setString(2, r.patientLastName);
			else
				ps.setNull(2, java.sql.Types.VARCHAR);
				
			ps.setLong(3, r.sampleId);
			
			if (r.samplesAdditionalInfoId != null)
				ps.setLong(4, r.samplesAdditionalInfoId);
			else
				ps.setNull(4, java.sql.Types.BIGINT);
				
			if (r.additionalInformation != null)
				ps.setString(5, r.additionalInformation);
			else
				ps.setNull(5, java.sql.Types.VARCHAR);
		}
	}

	/**
	 * JDBC statement builder for delete operations
	 */
	public static class SessionDeleteStatementBuilder implements JdbcStatementBuilder<SampleDetailsRecord> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(SessionDeleteStatementBuilder.class);

		@Override
		public void accept(PreparedStatement ps, SampleDetailsRecord r) throws SQLException {
			logger.info("Executing SQL delete for sample: sampleId={}", r.sampleId);
			ps.setLong(1, r.sampleId);
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
		public void open(org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext context)
				throws Exception {
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
            if (topic.endsWith(".Samples")) {
				if (node.hasNonNull("after")) {
					logger.info("Samples event: txId={}, op={}", txId, op);
                    return new TxEvent(txId, EventType.SAMPLES, (ObjectNode) node.get("after"), null);
				} else if (node.hasNonNull("before") && "d".equals(op)) {
					logger.info("Samples delete event: txId={}, op={}", txId, op);
                    return new TxEvent(txId, EventType.SAMPLES_DELETE, (ObjectNode) node.get("before"), null);
				}
			}
            if (topic.endsWith(".SamplesAdditionalInfo")) {
				if (("c".equals(op) || "u".equals(op)) && node.hasNonNull("after")) {
					ObjectNode after = (ObjectNode) node.get("after");
					Long additionalInfoId = after.has("Id") ? after.get("Id").asLong() : null;
                    Long sampleIdRef = after.has("SampleId") && !after.get("SampleId").isNull()
                            ? (after.get("SampleId").isNumber() ? after.get("SampleId").asLong()
                                    : parseLongSafe(after.get("SampleId").asText(), null))
                            : null;
					String additionalInformation = after.has("AdditionalInformation")
							? after.get("AdditionalInformation").asText()
							: null;
					logger.info("SamplesAdditionalInfo event: txId={}, op={}, addInfoId={}, sampleIdRef={}",
							txId, op, additionalInfoId, sampleIdRef);
					return new TxEvent(txId, EventType.SAMPLESADDITIONALINFO, null,
							new AdditionalInfo(additionalInfoId, additionalInformation, sampleIdRef));
				} else if (node.hasNonNull("before") && "d".equals(op)) {
					ObjectNode before = (ObjectNode) node.get("before");
					Long additionalInfoId = before.has("Id") ? before.get("Id").asLong() : null;
                    Long sampleIdRef = before.has("SampleId") && !before.get("SampleId").isNull()
                            ? (before.get("SampleId").isNumber() ? before.get("SampleId").asLong()
                                    : parseLongSafe(before.get("SampleId").asText(), null))
                            : null;
					logger.info("SamplesAdditionalInfo delete event: txId={}, op={}, addInfoId={}, sampleIdRef={}",
							txId, op, additionalInfoId, sampleIdRef);
					return new TxEvent(txId, EventType.SAMPLESADDITIONALINFO_DELETE, null,
							new AdditionalInfo(additionalInfoId, null, sampleIdRef));
				}
			}
		}
		logger.debug("No valid transaction event found for topic: {}, op: {}", topic, op);
		return null;
	}

	enum EventType {
		SAMPLES, SAMPLESADDITIONALINFO, SAMPLES_DELETE, SAMPLESADDITIONALINFO_DELETE, TX_END, TX_OTHER
	}

    static class AdditionalInfo {
        final Long additionalInfoId;
        final String additionalInformation;
        final Long sampleIdRef; // from SamplesAdditionalInfo.SampleId

        AdditionalInfo(Long additionalInfoId, String additionalInformation, Long sampleIdRef) {
            this.additionalInfoId = additionalInfoId;
            this.additionalInformation = additionalInformation;
            this.sampleIdRef = sampleIdRef;
        }
    }

    static class TxEvent {
		final String txId;
		final EventType type;
        final ObjectNode samplesAfter;
		final AdditionalInfo addInfo;

        TxEvent(String txId, EventType type, ObjectNode samplesAfter, AdditionalInfo addInfo) {
			this.txId = txId;
			this.type = type;
            this.samplesAfter = samplesAfter;
			this.addInfo = addInfo;
		}
	}

	static class SampleDetailsRecord {
		final String patientFirstName;
		final String patientLastName;
		final Long sampleId;
		final Long samplesAdditionalInfoId;
		final String additionalInformation;

		SampleDetailsRecord(String patientFirstName, String patientLastName, Long sampleId,
				Long samplesAdditionalInfoId, String additionalInformation) {
			this.patientFirstName = patientFirstName;
			this.patientLastName = patientLastName;
			this.sampleId = sampleId;
			this.samplesAdditionalInfoId = samplesAdditionalInfoId;
			this.additionalInformation = additionalInformation;
		}
	}

	enum SessionResultType {
		UPSERT, DELETE
	}

	static class SessionResult {
		final SessionResultType type;
		final SampleDetailsRecord record;
		final DeleteRecord deleteRecord;

		SessionResult(SessionResultType type, SampleDetailsRecord record, DeleteRecord deleteRecord) {
			this.type = type;
			this.record = record;
			this.deleteRecord = deleteRecord;
		}
	}

	static class DeleteRecord {
		final int customerId;

		DeleteRecord(int customerId) {
			this.customerId = customerId;
		}
	}

	/**
	 * JDBC statement builder for delete operations
	 */
	public static class DeleteStatementBuilder implements JdbcStatementBuilder<DeleteRecord> {
		private static final long serialVersionUID = 1L;
		private static final Logger logger = LoggerFactory.getLogger(DeleteStatementBuilder.class);

		@Override
		public void accept(PreparedStatement ps, DeleteRecord r) throws SQLException {
			logger.info("Executing SQL delete for customer: id={}", r.customerId);
			ps.setInt(1, r.customerId);
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

    private static Long parseLongSafe(String text, Long fallback) {
        try {
            return Long.parseLong(text);
        } catch (Exception e) {
            return fallback;
        }
    }
}
