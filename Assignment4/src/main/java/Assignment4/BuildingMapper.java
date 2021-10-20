package Assignment4;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import proto.building.Building;
import proto.building.BuildingList;

import java.util.Arrays;

import static Assignment4.util.Constants.*;

public class BuildingMapper extends Mapper<NullWritable, BytesWritable, ImmutableBytesWritable, Put> {


    ImmutableBytesWritable TABLE_NAME_TO_INSERT = new ImmutableBytesWritable(Bytes.toBytes(BUILDING_TABLE_NAME));

    public void map(NullWritable key, BytesWritable value, Context context) {
        try {

            byte b[]=value.getBytes();
            BuildingList buildingList=BuildingList.parseFrom(Arrays.copyOf(value.getBytes(), value.getLength()));

            for(Building building:buildingList.getBuildingsList()){
                int building_code=building.getBuildingCode();
                byte byteArray[]=building.toByteArray();
                Put put = new Put(Bytes.toBytes(building_code));
                put.addColumn(Bytes.toBytes(BUILDING), Bytes.toBytes(BUILDING_DETAILS), byteArray);
                context.write(TABLE_NAME_TO_INSERT, put);
            }
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}

