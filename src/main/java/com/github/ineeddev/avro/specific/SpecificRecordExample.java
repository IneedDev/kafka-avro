package com.github.ineeddev.avro.specific;

import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {
    public static void main(String[] args) {

                /*
        0 define schema
        1 specifc record
        2 write generic record to a file
        3 read generic record from a file
        4 interpret as a generic record
         */

        Customer.Builder customerBuilder =  Customer.newBuilder();
        customerBuilder.setAge(25);
        customerBuilder.setFirstName("Pawel");
        customerBuilder.setHeight(15);
        customerBuilder.setWeight(52);
        customerBuilder.setLastName("Romaniuk");
        Customer customer = customerBuilder.build();

        System.out.println(customer.toString());

        // write to file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);
        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)){
            dataFileWriter.create(customer.getSchema(),new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            System.out.println("Written customer-specific.avro");
        }catch (IOException e){
            e.getMessage();
        }

        // above procedure will generate avro file into target directory
        // to read it it's necessary encode it by DatumFilereader

        final  File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        final DataFileReader<Customer> dataFileReader;

        try {
            dataFileReader = new DataFileReader<>(file,datumReader);
            while (dataFileReader.hasNext()){
                Customer readCustomer = dataFileReader.next();
                System.out.println(readCustomer.toString());
                System.out.println("First name "+ customer.getFirstName());
            }
        }catch (IOException e){
            e.getMessage();
        }

    }
}
