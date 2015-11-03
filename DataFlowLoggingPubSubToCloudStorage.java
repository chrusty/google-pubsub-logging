package org.chrusty.dataflow.logging.cloudstorage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.PubsubIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.PCollection;

/*
* Takes log-messages from PUB/SUB
* Batches them up 
* Writes them to cloud-storage 
*/

public class DataFlowLoggingPubSubToCloudStorage {
    private static final Logger LOG = LoggerFactory.getLogger(DataFlowLoggingPubSubToCloudStorage.class);

    // Options:
    public static interface DataFlowLoggingOptions extends PipelineOptions {
      @Description("Pub/Sub topic")
      @Default.String("projects/PROJECT/topics/TOPIC")
      String getPubsubTopic();
      void setPubsubTopic(String topic);

      @Description("Path of the cloud-storage bucket to write to")
      @Default.String("gs://bucket/logs")
      String getOutput();
      void setOutput(String value);
    }
    
    public static void main(String[] args) throws Exception {
      // Options and parameters:
      DataFlowLoggingOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataFlowLoggingOptions.class);

      // Create a pipeline using these options:
      Pipeline pipeline = Pipeline.create(options);

      // Get input from a Pub/Sub topic:
      LOG.info("Reading from PubSub ...");      
      PCollection<String> input = pipeline.apply( PubsubIO.Read.topic( options.getPubsubTopic() ) );

      // Write to a text-file:
      LOG.info("Writing to text-file ...");
      input.apply(TextIO.Write.named("WriteLogs").to(options.getOutput()));
      
      // Run the pipeline:
      pipeline.run();
    }

}
