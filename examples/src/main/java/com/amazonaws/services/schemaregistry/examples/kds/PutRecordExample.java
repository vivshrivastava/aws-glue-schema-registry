/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates.
 * Licensed under the Apache License, Version 2.0 (the
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
package com.amazonaws.services.schemaregistry.examples.kds;

import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import com.amazonaws.services.schemaregistry.utils.AVROUtils;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;

/**
 * This is an example of how to use Glue Schema Registry (GSR) with Kinesis Data Streams Get / Put Record APIs.
 * This code is <b>not</b> applicable if you are using KCL / KPL libraries.
 * GSR is already available in KCL / KPL libraries. See, https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kds
 */
public class PutRecordExample {
    private static final String AVRO_USER_SCHEMA_FILE = "src/main/resources/user.avsc";
    private static AmazonKinesis kinesisClient;
    private static final Logger LOGGER = Logger.getLogger(PutRecordExample.class.getSimpleName());
    private static AwsCredentialsProvider awsCredentialsProvider =
            DefaultCredentialsProvider
                    .builder()
                    .build();
    private static GlueSchemaRegistrySerializer glueSchemaRegistrySerializer;

    public static void main(final String[] args) throws Exception {
        Options options = new Options();
        options.addOption("region", true, "Specify region");
        options.addOption("stream", true, "Specify stream");
        options.addOption("schema", true, "Specify schema");
        options.addOption("numRecords", true, "Specify number of records");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        if (!cmd.hasOption("stream")) {
            throw new IllegalArgumentException("Stream name needs to be provided.");
        }
        String defaultRegionName = "us-east-1";
        String defaultSchemaName = "testGsrSchema";
        String defaultStreamName = "";
        String defaultNumOfRecords = "10";

        String regionName = cmd.getOptionValue("region", defaultRegionName);
        String schemaName = cmd.getOptionValue("schema", defaultSchemaName);
        String streamName = cmd.getOptionValue("stream", defaultStreamName);
        int numOfRecords = Integer.parseInt(cmd.getOptionValue("numRecords", defaultNumOfRecords));

        //Kinesis data streams client initialization.
        kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(regionName).build();

        glueSchemaRegistrySerializer =
                new GlueSchemaRegistrySerializerImpl(
                        awsCredentialsProvider,
                        getSchemaRegistryConfiguration(regionName)
                );

        Schema gsrSchema =
                new Schema(getAvroSchema().toString(),
                        DataFormat.AVRO.name(), schemaName);

        LOGGER.info("Client initialization complete.");

        Date timestamp = DateTime.now().toDate();

        putRecordsWithSchema(streamName, numOfRecords, gsrSchema, timestamp);
    }

    private static void putRecordsWithSchema(String streamName, int numOfRecords, Schema gsrSchema, Date timestamp) {
        //Standard Kinesis code to putRecords into a Kinesis Data Stream.
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);

        List<PutRecordsRequestEntry> recordsRequestEntries = new ArrayList<>();

        LOGGER.info("Putting " + numOfRecords + " into " + streamName + " with schema " + gsrSchema);
        for (int i = 0; i < numOfRecords; i++) {
            GenericRecord record = (GenericRecord) getTestRecord(i);
            byte[] recordWithSchema = encodeRecord(record, streamName, gsrSchema);
            PutRecordsRequestEntry entry = new PutRecordsRequestEntry();
            entry.setData(ByteBuffer.wrap(recordWithSchema));
            entry.setPartitionKey(String.valueOf(timestamp.toInstant()
                    .toEpochMilli()));

            recordsRequestEntries.add(entry);
            LOGGER.info("Enter new entry =>" + entry.toString());
        }

        putRecordsRequest.setRecords(recordsRequestEntries);

        PutRecordsResult putRecordResult = kinesisClient.putRecords(putRecordsRequest);

        LOGGER.info("Successfully put records: " + putRecordResult);
    }

    private static byte[] encodeRecord(GenericRecord record, String streamName, Schema gsrSchema) {
        byte[] recordAsBytes = convertRecordToBytes(record);
        //Pass the GSR Schema and record payload to glueSchemaRegistrySerializer.encode method.
        byte[] recordWithSchemaHeader =
                glueSchemaRegistrySerializer.encode(streamName, gsrSchema, recordAsBytes);
        return recordWithSchemaHeader;
    }


    private static org.apache.avro.Schema getAvroSchema() {
        //Read Avro schema object from File.
        org.apache.avro.Schema avroSchema = null;
        try {
            avroSchema = new org.apache.avro.Schema.Parser().parse(new File(AVRO_USER_SCHEMA_FILE));
        } catch (IOException e) {
            LOGGER.warning("Error parsing Avro schema from file" + e.getMessage());
            throw new UncheckedIOException(e);
        }
        return avroSchema;
    }

    private static byte[] convertRecordToBytes(final Object record) {
        //Standard Avro code to convert records into bytes.
        ByteArrayOutputStream recordAsBytes = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(recordAsBytes, null);
        GenericDatumWriter datumWriter = new GenericDatumWriter<>(AVROUtils.getInstance().getSchema(record));
        try {
            datumWriter.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            LOGGER.warning("Failed to convert record to Bytes" + e.getMessage());
            throw new UncheckedIOException(e);
        }
        return recordAsBytes.toByteArray();
    }

    private static GenericRecord convertBytesToRecord(org.apache.avro.Schema avroSchema, byte[] record) {
        //Standard Avro code to convert bytes to records.
        final GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>(avroSchema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(record, null);
        GenericRecord genericRecord = null;
        try {
            genericRecord = datumReader.read(null, decoder);
        } catch (IOException e) {
            LOGGER.warning("Failed to convert bytes to record" + e.getMessage());
            throw new UncheckedIOException(e);
        }
        return genericRecord;
    }

    private static Map<String, String> getMetadata() {
        //Metadata is optionally used by GSR while auto-registering a new schema version.
        Map<String, String> metadata = new HashMap<>();
        metadata.put("event-source-1", "topic1");
        metadata.put("event-source-2", "topic2");
        metadata.put("event-source-3", "topic3");
        return metadata;
    }

    private static GlueSchemaRegistryConfiguration getSchemaRegistryConfiguration(String regionName) {
        GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(regionName);
        //Optional setting to enable auto-registration.
        configs.setSchemaAutoRegistrationEnabled(true);
        //Optional setting to define metadata for the schema version while auto-registering.
        configs.setMetadata(getMetadata());
        return configs;
    }

    private static GlueSchemaRegistryConfiguration getSchemaRegistryConfiguration(String regionName, String schemaName) {
        GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(regionName);
        //Optional setting to enable auto-registration.
        configs.setSchemaAutoRegistrationEnabled(true);
        //Optional setting to define metadata for the schema version while auto-registering.
        configs.setMetadata(getMetadata());
        return configs;
    }

    private static Object getTestRecord(int i) {
        //Creating some sample Avro records.
        GenericRecord genericRecord;
        genericRecord = new GenericData.Record(getAvroSchema());
        genericRecord.put("name", "testName" + i);
        //genericRecord.put("favorite_number", i);
        genericRecord.put("favorite_color", "color" + i);
        int randomNumber = new Random().nextInt(100) + 1;
        genericRecord.put("favorite_number", randomNumber);

        return genericRecord;
    }
}
