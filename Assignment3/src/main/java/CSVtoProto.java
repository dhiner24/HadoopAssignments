import protofiles.EmployeeBuilding;
import util.Constant;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class CSVtoProto {

    private void readBuildingData(String[] buildingData) {
        if (buildingData.length <= 0) return;

        protofiles.EmployeeBuilding.Building.Builder building = protofiles.EmployeeBuilding.Building.newBuilder();

        building.setBuildingCode(buildingData[0])
                .setTotalFloor(Integer.parseInt(buildingData[1]))
                .setNumCompanies(Integer.parseInt(buildingData[2]))
                .setCafeteriaCode(buildingData[3]);

        System.out.println(building.toString());

    }


    private void readEmployeeData(String[] empData) {
        if(empData.length <= 0) return;

        protofiles.EmployeeBuilding.Person.Builder employee = protofiles.EmployeeBuilding.Person.newBuilder();

        employee.setEmployeeID(Integer.parseInt(empData[0]))
                .setFirstName(empData[1])
                .setLastName(empData[2])
                .setEmail(empData[3])
                .setDepartment(empData[4])
                .setBuildingCode(empData[5])
                .setFloorNumberValue(Integer.parseInt(empData[6]));

        System.out.println(employee.toString());
    }

    //reading the CSV file and setting it to proto objects
    public void printData(String path , boolean isEmployee) {
        try {

            FileReader fileReader = new FileReader(path);
            BufferedReader buffReader = new BufferedReader(fileReader);
            String line ;
            boolean firstLine = true;

            // reading  csv file and setting to person proto
            while((line = buffReader.readLine()) != null)
            {
                if ( firstLine) {
                    firstLine = false;
                    continue;
                }
                String [] data = line.split(",");

                if (isEmployee)
                    readEmployeeData(data);
                else
                    readBuildingData(data);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
