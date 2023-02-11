package com.tridhyaintuit.section2;

import com.tridhyaintuit.section2.model.CustomerEntity;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        String outputPath = "/home/kathansoni/Downloads/output2.csv";

        PCollection<CustomerEntity> pList = pipeline.apply(Create.of(getCustomers()));

//        PCollection<String> pStrList = pList.apply(MapElements.into(TypeDescriptors.strings()).via(CustomerEntity::getName).via(CustomerEntity::getId));
        PCollection<String> pStrList = pList.apply(MapElements.into(TypeDescriptors.strings()).via((CustomerEntity customer) ->
                customer.getName() + "," + customer.getId()
        ));
        pStrList.apply(TextIO.write().to(outputPath).withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }

    static List<CustomerEntity> getCustomers(){

        CustomerEntity customerEntity1 = new CustomerEntity("1001","John");
        CustomerEntity customerEntity2 = new CustomerEntity("1002","Doe");
        List<CustomerEntity> customerEntities = new ArrayList<CustomerEntity>();

        customerEntities.add(customerEntity1);
        customerEntities.add(customerEntity2);

        return customerEntities;
    }
}
