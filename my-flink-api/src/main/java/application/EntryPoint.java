package application;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

public class EntryPoint {
	public static void main(String[] args) throws Exception {
	    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

	    DataSet<String> data = env.readTextFile("C:\\akash\\flink\\example\\inputFile.txt");
	    //CsvReader dataIn = env.readCsvFile("C:\\akash\\flink\\example\\hospitals_new.csv");//("file:///path/to/file");

	    data
	        .filter(new FilterFunction<String>() {
	            public boolean filter(String value) {
	                return value.matches("mumbai");//contains("NEW DELHI");
	            }
	        })
	        .writeAsText("C:\\akash\\flink\\example\\outputFile.txt");

	    JobExecutionResult res = env.execute();
	}
}
