package org.chrusty.dataflow.logging.bigquery;

import java.util.List;
import java.util.ArrayList;

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

      @Description("BigQuery table name")
      @Default.String("1970_01_01")
      String getBigQueryTable();
      void setBigQueryTable(String table);
    }
    
    // A DoFn that converts a logging-message into a BigQuery table row:
  static class FormatAsTableRowFn extends DoFn<String, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row = new TableRow()
          .set("time", "1")
          .set("host", "some-host")
          .set("ident", "syslog")
          .set("environment", "cruft")
          .set("role", "prawn")
          .set("message", c.element());
      c.output(row);
    }
    }

    // Return a reference to a BigQuery table:
    private static TableReference getTableReference(DataFlowLoggingOptions options) {
        TableReference tableRef = new TableReference();
        tableRef.setProjectId(options.getProject());
        tableRef.setDatasetId(options.getBigQueryDataset());
        tableRef.setTableId(options.getBigQueryTable());
        return tableRef;
  }

    // Return a BigQuery table-schema:
    private static TableSchema getSchema() {
        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("time").setType("STRING"));
        fields.add(new TableFieldSchema().setName("host").setType("STRING"));
        fields.add(new TableFieldSchema().setName("ident").setType("STRING"));
        fields.add(new TableFieldSchema().setName("environment").setType("STRING"));
        fields.add(new TableFieldSchema().setName("role").setType("STRING"));
        fields.add(new TableFieldSchema().setName("message").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);
        return schema;
    }

    // The fun starts here...    
    public static void main(String[] args) throws Exception {

      // Options and parameters:
      DataFlowLoggingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataFlowLoggingOptions.class);

      // Create a pipeline using these options:
      Pipeline pipeline = Pipeline.create(options);

      // Get input from a Pub/Sub topic:
      LOG.info(String.format("Reading from PubSub topic (%s) ...", options.getPubsubTopic()));
      PCollection<String> input = pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()));

      // Write to BigQuery:
      LOG.info(String.format("Writing to BigQuery (%s:%s) ...", options.getBigQueryDataset(), options.getBigQueryTable()));
      input.apply(ParDo.of(new FormatAsTableRowFn()))
      .apply(BigQueryIO.Write.to(getTableReference(options)).withSchema(getSchema()));
      
      // Run the pipeline:
      pipeline.run();
    }

}
