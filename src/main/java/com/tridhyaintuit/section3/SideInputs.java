package com.tridhyaintuit.section3;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;
import java.util.Objects;

public class SideInputs {
    public static void main(String[] args) {
        Pipeline p = Pipeline.create();

      PCollection<KV<String,String>> pReturn = p.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/return.csv"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {

                    @ProcessElement
                    public void process(ProcessContext c) {
                        String[] arr = Objects.requireNonNull(c.element()).split(",");
                        c.output(KV.of(arr[0], arr[1]));
                    }
                }));
            PCollectionView <Map<String,String>> pMap = pReturn.apply(View.asMap());

        PCollection<String> pCustomerList = p.apply(TextIO.read().from("/home/kathansoni/Documents/ApacheBeam/cust_order.csv"));

        pCustomerList.apply(ParDo.of(new DoFn<String, Void>() {

            @ProcessElement
            public void process(ProcessContext c){
               Map<String, String> pSideInputView =  c.sideInput(pMap);

              String[] arr = Objects.requireNonNull(c.element()).split(",");
               String custName = pSideInputView.get(arr[0]);
                if(custName==null){
                    System.out.println(c.element());
                }
            }
        }).withSideInputs(pMap));


        p.run();
    }
}
