package com.anmol;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import com.anmol.util.Constants;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

public class HbaseWriterMain {

    public Configuration config;

    //Constructor
    public HbaseWriterMain() {
        config = new Configuration();
    }

    public void createTable(String tableName) throws IOException {

        Configuration conf = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(conf);

        Admin hAdmin = connection.getAdmin();



        if (hAdmin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName + " already exists");
            return;
        }
        TableName tname = TableName.valueOf(tableName);
        TableDescriptorBuilder tableDescBuilder = TableDescriptorBuilder.newBuilder(tname);

        tableDescBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("Name".getBytes()).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("Age".getBytes()).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("Company".getBytes()).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("Building_code".getBytes()).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("Phone_Number".getBytes()).build())
                .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("Address".getBytes()).build())
                .build();

        hAdmin.createTable(tableDescBuilder.build());

        System.out.println(tableName + "table crated");

    }

    private HashMap<Integer, String> getColumnMapping() {
        HashMap<Integer, String> hm = new HashMap<>();
        hm.put(0, "Name");
        hm.put(1, "Age");
        hm.put(2, "Company");
        hm.put(3, "Building_code");
        hm.put(4, "Phone_Number");
        hm.put(5, "Address");
        return hm;
    }

     // making connection with HBase and inserting into HBase table
    private void insertDataToHbase(String[] record, int rowId) throws IOException {

        Table table = null;
        Connection connection = null;
        HashMap<Integer, String> hm = getColumnMapping();

        try
        {
            Configuration conf = HBaseConfiguration.create();
            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("people")); //Do not hard code table names.
            Put p = new Put(Bytes.toBytes(String.valueOf(rowId)));
            for(int i = 0; i < record.length; ++ i) {
                String qualifier = hm.get(i);
                if(qualifier != null)
                    p.addColumn(Bytes.toBytes(Constants.columnFamily),
                            Bytes.toBytes(qualifier),
                            Bytes.toBytes(record[i]));
            }

            table.put(p);

        }catch (Exception e) { //Catch specific exception instead of generic exception.
            e.printStackTrace(); 
        } finally {
            if(table != null)
                table.close();
            if(connection != null)
                connection.close();
        }
    }

    /*
    storeinHBASE() will read from HDFS files and will store the data in HBASE/
     */
    public void storeInHBASE(FileSystem hdfs, String uri) throws IOException { 

        config.set("fs.defaultFS", Constants.hdfsUrl);
        FileStatus[] fileStatus = hdfs.listStatus(new Path(uri));
        Path[] paths = FileUtil.stat2Paths(fileStatus);

        FileSystem fileSystem = FileSystem.get(config);

        int rowId = 1;
        for (Path path : paths) {
            FSDataInputStream inputStream = fileSystem.open(path);

            String line = IOUtils.toString(inputStream, Constants.encoding).split("\n")[1];
            String[] record = line.split(",");
            insertDataToHbase(record, rowId);
            rowId++;
            inputStream.close();
        }
        fileSystem.close();
    }

}
