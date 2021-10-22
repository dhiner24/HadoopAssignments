
package com.anmol;
import com.anmol.util.Constants;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Main {

    public static void main(String[] args) throws IOException, URISyntaxException {

        HbaseWriterMain obj = new HbaseWriterMain(); //use intellij suggested names, eg. hbaseWriterMain instead of obj.

        FileSystem fs = FileSystem.get(new URI(Constants.hdfsUrl),obj.config);

        String uri = Constants.hdfsUrl + Constants.internalUrl;
        obj.createTable("people");
        obj.storeInHBASE(fs , uri);
    }
}
