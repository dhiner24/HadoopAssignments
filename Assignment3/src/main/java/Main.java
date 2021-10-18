import util.Constant;

public class Main {

    public static void main(String[] args) {

        CSVtoProto dataHandler = new CSVtoProto();

        // printing employee csv in proto format
        dataHandler.printData(Constant.employeeCSVPath , true);

        //printing buikdind csv in proto format
        dataHandler.printData(Constant.buildingCSVPath ,false);
    }
}