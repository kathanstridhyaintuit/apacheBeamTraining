package com.tridhyaintuit.section2;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {
    public static void main(String[] args) {

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create(myOptions);
        String inputPath = "/home/kathansoni/Downloads/input.csv";
        String outputPath = "/home/kathansoni/Downloads/output.csv";

        PCollection<String> output = pipeline.apply(TextIO.read().from(myOptions.getInputFile()));

        output.apply(TextIO.write().to(myOptions.getOutputFile()).withNumShards(1).withSuffix(myOptions.getExtn()));
        pipeline.run();
        System.out.println("Pipeline created successfully and file created at path");
    }
}
