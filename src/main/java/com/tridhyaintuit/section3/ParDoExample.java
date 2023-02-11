package com.tridhyaintuit.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


class custFilter extends DoFn<String, String> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        String line = c.element();
        String arr[] = line.split(",");

        if(arr[3].equals("Los Angeles")){
            c.output(line);
        }
    }
}


public class ParDoExample {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();
        PCollection<String> pCustList = p.apply(TextIO.read().from("/home/kathansoni/Downloads/customer_pardo.csv"));

        PCollection<String> pOutput = pCustList.apply(ParDo.of(new custFilter()));

        pOutput.apply(TextIO.write().to("/home/kathansoni/Downloads/customer_pardo_output.csv").withHeader("Id,Name,Last Name, City").withNumShards(1).withSuffix(".csv"));
        p.run();

    }
}
