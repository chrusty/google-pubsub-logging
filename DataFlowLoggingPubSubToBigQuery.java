package org.chrusty.dataflow.logging.bigquery;

import java.util.List;
//import java.text.DateFormat;
import java.util.ArrayList;

//import org.joda.time.format.DateTimeFormat;
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
//import com.google.cloud.dataflow.sdk.transforms.SerializableFunction;
//import com.google.cloud.dataflow.sdk.transforms.windowing.BoundedWindow;
//import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows;
//import com.google.cloud.dataflow.sdk.transforms.windowing.CalendarWindows.DaysWindows;
//import com.google.cloud.dataflow.sdk.transforms.windowing.Window;
import com.google.cloud.dataflow.sdk.values.PCollection;

/*
* Takes log-messages from PUB/SUB
* Batches them up 
* Writes them to BigQuery
*/

public class DataFlowLoggingPubSubToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(DataFlowLoggingPubSubToBigQuery.class);
	private static JSONParser jsonParser = new JSONParser();
    
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
			JSONObject jsonMessage = null;
			try {
				// Parse the context as a JSON object:
				jsonMessage = (JSONObject) jsonParser.parse(c.element());

				// Make a BigQuery row from the JSON object:
				TableRow row = new TableRow()
						.set("time_stamp", jsonMessage.get("parsed_time"))
						.set("time_original", jsonMessage.get("time"))
						.set("host", jsonMessage.get("host"))
						.set("ident",jsonMessage.get("ident"))
						.set("environment", jsonMessage.get("environment"))
						.set("role", jsonMessage.get("role"))
						.set("message", jsonMessage.get("message"));

				// Output the row:
				c.output(row);
			} catch (ParseException e) {
				LOG.warn(String.format("Exception encountered parsing JSON (%s) ...", e));
			} catch (Exception e) {
				LOG.warn(String.format("Exception: %s", e));
			}
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

//    private SerializableFunction<BoundedWindow, String>() {
//        public String apply(BoundedWindow window) {
//            String dayString = DateTimeFormat.forPattern("yyyy_MM_dd").parseDateTime(
//              ((DaysWindows) window).getStartDate());
//            return "my-project:output.output_table_" + dayString;
//        }
//    }
//    
//    new SerializableFunction<BoundedWindow, String>() {
//        public String apply(BoundedWindow window) {
//          String dayString = DateTimeFormat.forPattern("yyyy_MM_dd").parseDateTime(
//            ((DaysWindows) window).getStartDate());
//          return "my-project:output.output_table_" + dayString;
//        }
//    }

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
    public static void main(String[] args) throws Exception {

      // Options and parameters:
      DataFlowLoggingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataFlowLoggingOptions.class);

      // Create a pipeline using these options:
      Pipeline pipeline = Pipeline.create(options);

      // Get input from a Pub/Sub topic:
      LOG.info(String.format("Reading from PubSub topic (%s) ...", options.getPubsubTopic()));
      PCollection<String> input = pipeline.apply(PubsubIO.Read.topic(options.getPubsubTopic()));

      // Write to BigQuery (static table):
      LOG.info(String.format("Writing to BigQuery (%s:%s) ...", options.getBigQueryDataset(), options.getBigQueryTable()));
      input.apply(ParDo.of(new FormatAsTableRowFn()))
      	.apply(BigQueryIO.Write.to(getTableReference(options))
      	  .withSchema(getSchema())
      		.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
      	      .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));

//      // Write to BigQuery (table-per-day):
//      // https://cloud.google.com/dataflow/java-sdk/JavaDoc/com/google/cloud/dataflow/sdk/io/BigQueryIO
//      input.apply(ParDo.of(new FormatAsTableRowFn()))
//      	.apply(Window.<TableRow>into(CalendarWindows.days(1)))
//      	  .apply(BigQueryIO.Write
//      	  .named("Write")
//      	  .withSchema(getSchema())
//      	  .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
//      	  .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//      	  .to(new SerializableFunction<BoundedWindow, String>() {
//            public String apply(BoundedWindow window) {
//              String dayString = ((BoundedWindow) window).TIMESTAMP_MIN_VALUE.toDateTime().toString("yyyy_MM_dd");
//              return String.format("ravelin-logging:logs.daily_%s", dayString);
//              //return String.format("%s:%s.%s_%s", options.getProject(), options.getBigQueryDataset(), options.getBigQueryTable(), dayString);
//            }
//          }
//      	)
//      );
      
      // Run the pipeline:
      pipeline.run();
    }

}

