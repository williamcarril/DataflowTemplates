/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.mongodb.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.JavascriptDocumentTransformerOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.MongoDbAggregateOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.MongoDbOptions;
import com.google.cloud.teleport.v2.mongodb.templates.MongoDbToBigQuery.Options;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiBatchOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptDocumentTransformer.TransformDocumentViaJavascript;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.MatchResult.Metadata;
import org.apache.beam.sdk.io.fs.MatchResult.Status;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.mongodb.AggregationQuery;
import org.apache.beam.sdk.io.mongodb.MongoDbIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.io.CharStreams;
import org.bson.BsonDocument;
import org.bson.Document;
import org.openjdk.nashorn.api.scripting.ScriptObjectMirror;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MongoDbToBigQuery} pipeline is a batch pipeline which ingests data from MongoDB and
 * outputs the resulting records to BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/mongodb-to-googlecloud/README_MongoDB_to_BigQuery.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "MongoDB_to_BigQuery",
    category = TemplateCategory.BATCH,
    displayName = "MongoDB to BigQuery",
    description =
        "The MongoDB to BigQuery template is a batch pipeline that reads documents from MongoDB and writes them to "
            + "BigQuery as specified by the <code>userOption</code> parameter.",
    optionsClass = Options.class,
    flexContainerName = "mongodb-to-bigquery",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The target BigQuery dataset must exist.",
      "The source MongoDB instance must be accessible from the Dataflow worker machines."
    })
public class MongoDbToBigQuery {
  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbToBigQuery.class);

  /**
   * Options supported by {@link MongoDbToBigQuery}
   *
   * <p>Inherits standard configuration options.
   */
  public interface Options
      extends PipelineOptions,
          MongoDbOptions,
          MongoDbAggregateOptions,
          BigQueryWriteOptions,
          BigQueryStorageApiBatchOptions,
          JavascriptDocumentTransformerOptions {}

  private static class ParseAsDocumentsFn extends DoFn<String, Document> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }

  public static void main(String[] args)
      throws ScriptException, IOException, NoSuchMethodException {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

    BigQueryIOUtils.validateBQStorageApiOptionsBatch(options);

    run(options);
  }

  public static boolean run(Options options)
      throws ScriptException, IOException, NoSuchMethodException {

    LOGGER.debug("Initializing workflow");
    Pipeline pipeline = Pipeline.create(options);
    String userOption = options.getUserOption();

    TableSchema bigquerySchema;

    // Get MongoDbUri plain text or base64 encrypted with a specific KMS encryption key
    String mongoDbUri = maybeDecrypt(options.getMongoDbUri(), options.getKMSEncryptionKey()).get();

    if (options.getJavascriptDocumentTransformFunctionName() != null
        && options.getJavascriptDocumentTransformGcsPath() != null) {
      bigquerySchema =
          MongoDbUtils.getTableFieldSchemaForUDF(
              mongoDbUri,
              options.getDatabase(),
              options.getCollection(),
              options.getJavascriptDocumentTransformGcsPath(),
              options.getJavascriptDocumentTransformFunctionName(),
              options.getUserOption());
    } else {
      bigquerySchema =
          MongoDbUtils.getTableFieldSchema(
              mongoDbUri, options.getDatabase(), options.getCollection(), options.getUserOption());
    }
    LOGGER.debug("Fetching MongoDB aggregate query");

    AggregationQuery aggregatePipeline =
        getAggregatePipeline(
            options.getAggregatePipelineFactoryGcpPath(),
            options.getAggregatePipelineFactoryFunctionName());

    LOGGER.debug("MongoDB aggregate query found: " + aggregatePipeline.toString());

    pipeline
        .apply(
            "Read Documents",
            MongoDbIO.read()
                .withUri(mongoDbUri)
                .withDatabase(options.getDatabase())
                .withCollection(options.getCollection())
                .withQueryFn(aggregatePipeline))
        .apply(
            ParDo.of(
                new DoFn<Document, Document>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    LOGGER.debug("Document extracted: " + document.toJson());
                    c.output(document);
                  }
                }))
        .apply(
            "UDF",
            TransformDocumentViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptDocumentTransformGcsPath())
                .setFunctionName(options.getJavascriptDocumentTransformFunctionName())
                .build())
        .apply(
            ParDo.of(
                new DoFn<Document, Document>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    LOGGER.debug("Document transformed: " + document.toJson());
                    c.output(document);
                  }
                }))
        .apply(
            "Transform to TableRow",
            ParDo.of(
                new DoFn<Document, TableRow>() {

                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    TableRow row = MongoDbUtils.getTableSchema(document, userOption);

                    LOGGER.debug("Table row to be loaded: " + row.toString());
                    c.output(row);
                  }
                }))
        .apply(
            "Write to Bigquery",
            BigQueryIO.writeTableRows()
                .to(options.getOutputTableSpec())
                .withSchema(bigquerySchema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    pipeline.run();
    return true;
  }

  private static AggregationQuery getAggregatePipeline(String path, String functionName)
      throws IOException, ScriptException, NoSuchMethodException {
    MatchResult fileMatch = FileSystems.match(path);
    checkArgument(
        fileMatch.status() == Status.OK && !fileMatch.metadata().isEmpty(),
        "Failed to match any files with the pattern: " + path);

    List<String> scripts =
        fileMatch.metadata().stream()
            .filter(metadata -> metadata.resourceId().getFilename().endsWith(".js"))
            .map(Metadata::resourceId)
            .map(
                resourceId -> {
                  try (Reader reader =
                      Channels.newReader(
                          FileSystems.open(resourceId), StandardCharsets.UTF_8.name())) {
                    return CharStreams.toString(reader);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .collect(Collectors.toList());

    ScriptEngineManager manager = new ScriptEngineManager();
    ScriptEngine engine = manager.getEngineByName("JavaScript");

    if (engine == null) {
      List<String> availableEngines = new ArrayList<>();
      for (ScriptEngineFactory factory : manager.getEngineFactories()) {
        availableEngines.add(factory.getEngineName() + " " + factory.getEngineVersion());
      }
      throw new RuntimeException(
          String.format("JavaScript engine not available. Found engines: %s.", availableEngines));
    }

    for (String script : scripts) {
      engine.eval(script);
    }

    Invocable invocable = (Invocable) engine;

    Object rawResult;
    synchronized (invocable) {
      rawResult = invocable.invokeFunction(functionName);
    }
    if (rawResult == null
        || ScriptObjectMirror.isUndefined(rawResult)
        || !(rawResult instanceof String)) {
      String className = "null";
      if (!(rawResult == null || ScriptObjectMirror.isUndefined(rawResult))) {
        className = rawResult.getClass().getName();
      }
      throw new RuntimeException(
          "Aggregate pipeline UDF Function did not return a String. Instead got: " + className);
    }

    ObjectMapper mapper = new ObjectMapper();

    Object[] result = mapper.readValue((String) rawResult, Object[].class);

    List<BsonDocument> stages = new ArrayList<BsonDocument>();

    for (Object e : result) {
      stages.add(BsonDocument.parse(mapper.writeValueAsString(e)));
    }

    return AggregationQuery.create().withMongoDbPipeline(stages);
  }
}
