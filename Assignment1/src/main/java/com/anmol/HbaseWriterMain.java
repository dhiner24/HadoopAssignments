package com.anmol;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;

import com.anmol.util.Constants;

public class HbaseWriterMain {

    public Configuration config;

    //Constructor
    public HbaseWriterMain() {
        config = new Configuration();
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
            table = connection.getTable(TableName.valueOf("people"));
            Put p = new Put(Bytes.toBytes(String.valueOf(rowId)));
            for(int i = 0; i < record.length; ++ i) {
                String qualifier = hm.get(i);
                if(qualifier != null)
                    p.addColumn(Bytes.toBytes(Constants.columnFamily),
                            Bytes.toBytes(qualifier),
                            Bytes.toBytes(record[i]));
            }

            table.put(p);

        }catch (Exception e) {
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
