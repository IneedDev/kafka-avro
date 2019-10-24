package com.guthub.ineeddev.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.io.IOException;

public class GenericRecordsExamples {

    public static void main(String[] args) {

        /*
        0 define schema
        1 generic record
        2 write generic record to a file
        3 read generic record from a file
        4 interpret as a generic record
         */

        Schema.Parser parser = new Schema.Parser();
        // we can parse string or file
        Schema schema = parser.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"doc\": \"Avro Schema for our Customer\",     \n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");
        // we build our first customer
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 26);
        customerBuilder.set("height", 175f);
        customerBuilder.set("weight", 70.5f);
        customerBuilder.set("automated_email", false);

        GenericData.Record myCustomer = customerBuilder.build();
        System.out.println(myCustomer);

        // example with default for automated email

        GenericRecordBuilder customerBuilderWithDefault = new GenericRecordBuilder(schema);
        customerBuilderWithDefault.set("first_name", "Pawel");
        customerBuilderWithDefault.set("last_name", "Doe");
        customerBuilderWithDefault.set("age", 26);
        customerBuilderWithDefault.set("height", 175f);
        customerBuilderWithDefault.set("weight", 70.5f);

        GenericData.Record myCustomerWitDefault = customerBuilderWithDefault.build();
        System.out.println(myCustomerWitDefault);

        // write to file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.create(schema,new File("customer-generic.avro"));
            dataFileWriter.append(myCustomer);
            System.out.println("Written customer-generic.avro");
        }catch (IOException e){
            e.getMessage();
        }
        // above procedure will generate avro file into target directory
        // to read it it's necessary encode it by DatumFilereader

        final  File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        GenericRecord customerRead;

        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file,datumReader)){
            customerRead = dataFileReader.next();
            System.out.println("ok read avro file");
            System.out.println(customerRead.toString());
            System.out.println(customerRead.get("first_name"));
        }catch (IOException e){
            e.getMessage();
        }

    }
}
