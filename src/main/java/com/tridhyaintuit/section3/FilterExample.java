package com.tridhyaintuit.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableBiFunction;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

class MyFilter implements SerializableFunction<String, Boolean> {


    @Override
    public Boolean apply(String input) {
        return input.contains("Los Angeles");
    }
}
public class FilterExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> pCustList = pipeline.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/customer_section4.csv"));

        // Using Filter
        PCollection<String> pOutput = pCustList.apply(Filter.by(new MyFilter()));
        pOutput.apply(TextIO.write().to("/home/kathansoni/Documents/ApacheBeam/customer_Filter_output.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pipeline.run();

    }
}
