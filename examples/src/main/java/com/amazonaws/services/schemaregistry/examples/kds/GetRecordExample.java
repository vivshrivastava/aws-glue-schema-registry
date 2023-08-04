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
import com.amazonaws.services.kinesis.model.*;
import com.amazonaws.services.schemaregistry.common.Schema;
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializer;
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializerImpl;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializer;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializerImpl;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.joda.time.DateTime;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.glue.model.DataFormat;

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
public class GetRecordExample {
    private static final String AVRO_USER_SCHEMA_FILE = "src/main/resources/user.avsc";
    private static AmazonKinesis kinesisClient;
    private static final Logger LOGGER = Logger.getLogger(GetRecordExample.class.getSimpleName());
    private static AwsCredentialsProvider awsCredentialsProvider =
            DefaultCredentialsProvider
                    .builder()
                    .build();
    private static GlueSchemaRegistrySerializer glueSchemaRegistrySerializer;
    private static GlueSchemaRegistryDeserializer glueSchemaRegistryDeserializer;

    public static void main(final String[] args) throws Exception {
        Options options = new Options();
        options.addOption("region", true, "Specify region");
        options.addOption("stream", true, "Specify stream");
        options.addOption("schema", true, "Specify schema");
        options.addOption("numRecords", true, "Specify number of records");
        options.addOption("useTagBasedLookup", true, "Specify useTagBasedLookup");
        options.addOption("registryName", true, "Specify registryName");
        options.addOption("metadataTagKeyName", true, "Specify metadataTagKeyName");
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

//        if (!cmd.hasOption("stream")) {
//            throw new IllegalArgumentException("Stream name needs to be provided.");
//        }

        String defaultRegionName = "us-east-1";
        String defaultSchemaName = "testGsrSchema";
        String defaultStreamName = "default-stream";
        String defaultNumOfRecords = "10";
        String defaultUseTagBasedLookup = "true";
        String defaultRegistryName = "default-registry";
        String defaultMetadataTagKeyName = "schema_id";

        String regionName = cmd.getOptionValue("region", defaultRegionName);
        String schemaName = cmd.getOptionValue("schema", defaultSchemaName);
        String streamName = cmd.getOptionValue("stream", defaultStreamName);
        boolean useTagBasedLookup = Boolean.parseBoolean(cmd.getOptionValue("useTagBasedLookup", defaultUseTagBasedLookup));
        String registryName = cmd.getOptionValue("registryName", defaultRegistryName);
        String metadataTagKeyName = cmd.getOptionValue("metadataTagKeyName", defaultMetadataTagKeyName);

        kinesisClient = AmazonKinesisClientBuilder.standard().withRegion(regionName).build();

//        glueSchemaRegistrySerializer =
//                new GlueSchemaRegistrySerializerImpl(
//                        awsCredentialsProvider,
//                        getSchemaRegistryConfiguration(
//                                regionName, useTagBasedLookup, schemaName, registryName, metadataTagKeyName)
//                );

        glueSchemaRegistryDeserializer =
                new GlueSchemaRegistryDeserializerImpl(awsCredentialsProvider, getSchemaRegistryConfiguration(regionName, useTagBasedLookup, schemaName, registryName, metadataTagKeyName));

        LOGGER.info("Client initialization complete.");
        getRecordsWithSchema(streamName);
    }


    private static void getRecordsWithSchema(String streamName) throws IOException {
        //Standard Kinesis code to getRecords from a Kinesis Data Stream.
        String shardIterator;
        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(streamName);
        List<Shard> shards = new ArrayList<>();
        LOGGER.info("Ready to get Records with Schema ");
        DescribeStreamResult streamRes;
        do {
            streamRes = kinesisClient.describeStream(describeStreamRequest);
            shards.addAll(streamRes.getStreamDescription().getShards());
            System.out.println("Shard size is " + shards.size());
            if (shards.size() > 0) {
                shards.get(shards.size() - 1).getShardId();
            }
        } while (streamRes.getStreamDescription().getHasMoreShards());

        for (Shard shard : shards) {
            System.out.println("Processing data from shard " + shard.getShardId());
            GetShardIteratorRequest itReq = new GetShardIteratorRequest();
            itReq.setStreamName(streamName);
            itReq.setShardId(shard.getShardId());
            itReq.setShardIteratorType("TRIM_HORIZON");

            GetShardIteratorResult shardIteratorResult = kinesisClient.getShardIterator(itReq);
            shardIterator = shardIteratorResult.getShardIterator();

            GetRecordsRequest recordsRequest = new GetRecordsRequest();
            recordsRequest.setShardIterator(shardIterator);
            recordsRequest.setLimit(10000);

            GetRecordsResult result = kinesisClient.getRecords(recordsRequest);
            Integer counter = 0;
            for (Record record : result.getRecords()) {
                ByteBuffer recordAsByteBuffer = record.getData();
                GenericRecord decodedRecord = decodeRecord(recordAsByteBuffer);
                counter = counter + 1;
                LOGGER.info("Decoded Record: " + counter + " " + decodedRecord);
            }
        }
    }

    private static GenericRecord decodeRecord(ByteBuffer recordByteBuffer) throws IOException {

        //Copy the data to a mutable buffer.
        byte[] recordWithSchemaHeaderBytes = new byte[recordByteBuffer.remaining()];
        recordByteBuffer.get(recordWithSchemaHeaderBytes, 0, recordWithSchemaHeaderBytes.length);

        Schema awsSchema =
                glueSchemaRegistryDeserializer.getSchema(recordWithSchemaHeaderBytes);

        byte[] data = glueSchemaRegistryDeserializer.getData(recordWithSchemaHeaderBytes);

        GenericRecord genericRecord = null;
        if (DataFormat.AVRO.name().equals(awsSchema.getDataFormat())) {
            org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(awsSchema.getSchemaDefinition());
            genericRecord = convertBytesToRecord(avroSchema, data);
        }
        return genericRecord;
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
        configs.setSchemaAutoRegistrationEnabled(true);
        configs.setMetadata(getMetadata());
        return configs;
    }

    private static GlueSchemaRegistryConfiguration getSchemaRegistryConfiguration(
            String regionName,
            boolean useTagBasedLookup,
            String schemaName,
            String registryName,
            String metadataTagKeyName
    ) {

        GlueSchemaRegistryConfiguration configs = new GlueSchemaRegistryConfiguration(regionName);
        configs.setUseTagBasedLookup(useTagBasedLookup);
        configs.setSchemaName(schemaName);
        configs.setRegistryName(registryName);
        configs.setMetadataTagKeyName(metadataTagKeyName);
        configs.setSchemaAutoRegistrationEnabled(true);
        configs.setMetadata(getMetadata());
        return configs;
    }

    private static Object getTestRecord(int i) {
        //Creating some sample Avro records.
        GenericRecord genericRecord;
        genericRecord = new GenericData.Record(getAvroSchema());
        genericRecord.put("name", "testName" + i);
        genericRecord.put("favorite_number", i);
        genericRecord.put("favorite_color", "color" + i);

        return genericRecord;
    }
}
