package Assignment4;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws InterruptedException, ClassNotFoundException, IOException {
        FilesToHDFS filesToHDFS = new FilesToHDFS();

        // this will store serialized file in hdfs and will create table in hbase
        filesToHDFS.storeFiles();

        MapDriver mapDriver = new MapDriver();

        // this will create hfile for employee and building
        // and will run mapper job to upload both table in hbase
        mapDriver.runDriver();
    }
}
