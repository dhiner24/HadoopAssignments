package Assignment4b;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import proto.building.Building;
import proto.employee.Employee;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static Assignment4b.util.Constants.*;

public class ReducerClass extends TableReducer<IntWritable, Result, ImmutableBytesWritable> {

    public ReducerClass(){
        System.out.println("Reached Reducer Class");
    }

    @Override
    protected void reduce(IntWritable key, Iterable<Result> values, Reducer<IntWritable, Result, ImmutableBytesWritable, Mutation>.Context context) throws IOException, InterruptedException {
        int cafeteria_code = 0;
        List<Employee> employeeList = new ArrayList();
        for (Result result : values) {
            if (result.containsColumn(Bytes.toBytes(EMPLOYEE), Bytes.toBytes(EMPLOYEE_DETAILS))) {
                Employee employee = Employee.parseFrom(result.value());
                employeeList.add(employee);
            }
            if (result.containsColumn(Bytes.toBytes(BUILDING), Bytes.toBytes(BUILDING_DETAILS))) {
                Building building = Building.parseFrom(result.value());
                cafeteria_code = building.getCafeteriaCode();
                int building_id = building.getBuildingCode();
            }
        }

        for (Employee employee : employeeList) {
            Put put = enrichCafeteriaCode(employee, cafeteria_code);
            int employee_id = employee.getEmployeeId();
            context.write(new ImmutableBytesWritable(Bytes.toBytes(employee_id)), put);

        }
    }

    public Put enrichCafeteriaCode(Employee employee, int cafe_value) {
        employee = employee.toBuilder().setCafeteriaCode(cafe_value).build();
        String name = employee.getName();
        int employee_id = employee.getEmployeeId();
        int building_code = employee.getBuildingCode();
        int salary = employee.getSalary();
        String department = employee.getDepartment();
        int floor = employee.getFloorNumber().getNumber();
        int cafeteria_code = employee.getCafeteriaCode();
        //System.out.println("emp_id= "+employee_id+" caf="+cafeteria_code);
        System.out.println("employee with Cafeteria code : " + employee);
        Put put = new Put(Bytes.toBytes(employee_id));
        put.addColumn(Bytes.toBytes("name"), Bytes.toBytes("name"), Bytes.toBytes(name));
        put.addColumn(Bytes.toBytes("building_code"), Bytes.toBytes("building_code"), Bytes.toBytes(building_code + ""));
        put.addColumn(Bytes.toBytes("employee_id"), Bytes.toBytes("employee_id"), Bytes.toBytes(employee_id + ""));
        put.addColumn(Bytes.toBytes("salary"), Bytes.toBytes("salary"), Bytes.toBytes(salary + ""));
        put.addColumn(Bytes.toBytes("department"), Bytes.toBytes("department"), Bytes.toBytes(department + ""));
        put.addColumn(Bytes.toBytes("floor_number"), Bytes.toBytes("floor"), Bytes.toBytes(floor + ""));
        put.addColumn(Bytes.toBytes("cafeteria_code"), Bytes.toBytes("cafeteria_code"), Bytes.toBytes(cafeteria_code + ""));
        return put;
    }
}
