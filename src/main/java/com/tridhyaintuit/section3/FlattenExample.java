package com.tridhyaintuit.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
    public static void main(String[] args) {
        Pipeline pipeline =  Pipeline.create();
        PCollection<String> pCustList1 = pipeline.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/customer_1.csv"));
        PCollection<String> pCustList2 = pipeline.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/customer_2.csv"));
        PCollection<String> pCustList3 = pipeline.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/customer_3.csv"));

        PCollectionList<String> list = PCollectionList.of(pCustList1).and(pCustList2).and(pCustList3);

        PCollection<String> mergedPcollectionObejct = list.apply(Flatten.pCollections());
        mergedPcollectionObejct.apply(TextIO.write().to("/home/kathansoni/Documents/ApacheBeam/customer_flattern_output.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pipeline.run();

    }
}
