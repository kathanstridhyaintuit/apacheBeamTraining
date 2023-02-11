package com.tridhyaintuit.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElementsExample {
    public static void main(String[] args) {

        Pipeline p = Pipeline.create();

        PCollection<String> pCustList = p.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/customer.csv"));

        //Using TypeDescriptors

        PCollection<String> pOutput = pCustList.apply(MapElements.into(TypeDescriptors.strings()).via(String::toUpperCase));

        pOutput.apply(TextIO.write().to("/home/kathansoni/Documents/ApacheBeam/cust_output.csv").withNumShards(1).withSuffix(".csv"));

        p.run();

    }
}
