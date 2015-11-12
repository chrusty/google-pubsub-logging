package org.chrusty.dataflow.logging.bigquery;

import java.util.List;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;

/*
 * Takes log-messages from PUB/SUB
 * Batches them up 
 * Writes them to BigQuery
 */

public class DataFlowLoggingPubSubToBigQuery {
  private static final Logger LOG = LoggerFactory.getLogger(DataFlowLoggingPubSubToBigQuery.class);

  // Options:
  public static interface DataFlowLoggingOptions extends DataflowPipelineOptions {
    @Description("Pub/Sub topic")
    @Default.String("projects/PROJECT/topics/TOPIC")
    String getPubsubTopic();
    void setPubsubTopic(String topic);

    @Description("BigQuery dataset name")
    @Default.String("logs")
    String getBigQueryDataset();
    void setBigQueryDataset(String dataset);

    @Description("BigQuery table prefix")
    @Default.String("daily")
    String getBigQueryTablePrefix();
    void setBigQueryTablePrefix(String tableprefix);

    @Description("BigQuery table suffix")
    @Default.String("yyyyMMdd")
    String getBigQueryTableSuffix();
    void setBigQueryTableSuffix(String tablesuffix);

    @Description("Split tables")
    @Default.Boolean(true)
    Boolean getSplitTables();
    void setSplitTables(Boolean splittables);
  }

  // A DoFn that converts a logging-message into a BigQuery table row:
  @SuppressWarnings("serial")
  static class FormatAsTableRowFn extends DoFn<String, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      JSONParser jsonParser = new JSONParser();
      JSONObject jsonMessage = null;
      TableRow row = null;

      try {
        // Parse the context as a JSON object:
        jsonMessage = (JSONObject) jsonParser.parse(c.element());

        // Make a BigQuery row from the JSON object:
        row = new TableRow()
            .set("time_stamp", jsonMessage.get("parsed_time"))
            .set("time_original", JSONObject.escape((String) jsonMessage.get("time")))
            .set("host", JSONObject.escape((String) jsonMessage.get("host")))
            .set("ident", JSONObject.escape((String) jsonMessage.get("ident")))
            .set("environment", JSONObject.escape((String) jsonMessage.get("environment")))
            .set("role", JSONObject.escape((String) jsonMessage.get("role")))
            .set("message", JSONObject.escape((String) jsonMessage.get("message")));

      } catch (ParseException e) {
        LOG.warn(String.format("Exception encountered parsing JSON (%s) ...", e));

        // Make a BigQuery row containing the exception:
        row = new TableRow()
            .set("host", "ParseException")
            .set("environment", e.toString())
            .set("ident", "Exception")
            .set("message", c.element());
      } catch (Exception e) {
        LOG.warn(String.format("Exception: %s", e));

        // Make a BigQuery row containing the exception:
        row = new TableRow()
            .set("host", "Exception")
            .set("environment", e.toString())
            .set("ident", "Exception")
            .set("message", c.element());
      } finally {
        // Output the row:
        c.output(row);
      }
    }
  }

  // Return a reference to a BigQuery table:
  private static TableReference getTableReference(DataFlowLoggingOptions options) {
    TableReference tableRef = new TableReference();
    tableRef.setProjectId(options.getProject());
    tableRef.setDatasetId(options.getBigQueryDataset());
    tableRef.setTableId(options.getBigQueryTablePrefix());
    return tableRef;
  }

  // Return a BigQuery table-schema:
  private static TableSchema getSchema() {
    List<TableFieldSchema> fields = new ArrayList<>();
    fields.add(new TableFieldSchema().setName("time_stamp").setType("TIMESTAMP"));
    fields.add(new TableFieldSchema().setName("time_original").setType("STRING"));
    fields.add(new TableFieldSchema().setName("host").setType("STRING"));
    fields.add(new TableFieldSchema().setName("ident").setType("STRING"));
    fields.add(new TableFieldSchema().setName("environment").setType("STRING"));
    fields.add(new TableFieldSchema().setName("role").setType("STRING"));
    fields.add(new TableFieldSchema().setName("message").setType("STRING"));
    TableSchema schema = new TableSchema().setFields(fields);
    return schema;
  }

  // The fun starts here...    
  @SuppressWarnings("serial")
  public static void main(String[] args) throws Exception {

    // Options and parameters:
    DataFlowLoggingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataFlowLoggingOptions.class);

    // Create a pipeline using these options:
    Pipeline pipeline = Pipeline.create(options);

    // Get input from a Pub/Sub topic:
    LOG.info(String.format("Reading from PubSub topic (%s) ...", options.getPubsubTopic()));
    PCollection<String> input = pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()));

    // See if we've been asked to split the tables up in BigQuery, or to simply write to one big table:
    if(options.getSplitTables()) {

      // Write to BigQuery (table-per-day):
      String formattedTablePrefix = String.format("%s:%s.%s", options.getProject(), options.getBigQueryDataset(), options.getBigQueryTablePrefix());
      String formattedTableSuffix = options.getBigQueryTableSuffix();
      LOG.info(String.format("Writing to BigQuery (%s_%s) ...", formattedTablePrefix, formattedTableSuffix));
      input.apply(ParDo.of(new FormatAsTableRowFn()))
      .apply(Window.<TableRow>into(CalendarWindows.days(1)))
      .apply(BigQueryIO.Write
          .named("Write")
          .withSchema(getSchema())
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
          .to(new SerializableFunction<BoundedWindow, String>() {
            public String apply(BoundedWindow window) {
              // Stream logs into a new table every hour (based on the time they're received from PUB/SUB, NOT the time-stamps):
              // A crufty broken example of proper windowing can be found here https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/BigQueryIO
              SimpleDateFormat sdf = new SimpleDateFormat(formattedTableSuffix);
              String tableSuffixString = sdf.format(new Date());

              // Return a table-reference:
              return String.format("%s_%s", formattedTablePrefix, tableSuffixString);
            }
          }
              )
          );
    } else {
      // Write to BigQuery (static table):
      LOG.info(String.format("Writing to BigQuery (%s:%s) ...", options.getBigQueryDataset(), options.getBigQueryTablePrefix()));
      input.apply(ParDo.of(new FormatAsTableRowFn()))
      .apply(BigQueryIO.Write.to(getTableReference(options))
          .withSchema(getSchema())
          .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
          .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));       
    }

    // Run the pipeline:
    pipeline.run();
  }

}
