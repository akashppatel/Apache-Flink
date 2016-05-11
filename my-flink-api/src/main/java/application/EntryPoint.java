package application;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;

public class EntryPoint {
	

	public static void main(String[] args) throws Exception {
	    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

	    //readHospitalAsText(env);
	    
	    readHospitalAsCsv(env);
	   
	    JobExecutionResult res = env.execute();
	}

	public static void readHospitalAsCsv(ExecutionEnvironment env)
			throws Exception {
		CsvReader dataIn = env.readCsvFile("./data/input.csv");//("file:///path/to/file");

		// dataIn.types(Class<Integer> id, Class<String> hospital, Class<String> , Class<String> , Class<String>);
		DataSource<Tuple5<Long, String, String, String, String>> csvFields = 
				dataIn.ignoreFirstLine().ignoreInvalidLines().types(long.class,String.class,String.class,String.class,String.class);

		csvFields.count();
		csvFields
		/*.map(new MapFunction<Tuple5<Long,String,String,String,String>, Tuple2<Long,String>>() {

			public Tuple2<Long,String> map(Tuple5<Long, String, String, String, String> inputs)
					throws Exception {
				// TODO Auto-generated method stub
				String address = inputs.f1.trim();

				return new Tuple2<Long, String>(inputs.f0,address);
			}
		})*/
		.writeAsCsv("./data/sumOutputFile");
	}

	public static void readHospitalAsText(ExecutionEnvironment env) {
		//DataSet<String> data = env.readTextFile("C:\\akash\\flink\\example\\inputFile.txt");
	    DataSet<String> data = env.readTextFile("./data/input.csv");

	    //CsvReader dataIn = env.readCsvFile("C:\\akash\\flink\\example\\input.csv");//("file:///path/to/file");
	
	    // Create a DataStream from a list of elements
	    /*DataSource <Integer> data = env.fromElements(1, 2, 3, 4, 5);
	    data.count();
	    data.print();*/
	    
	    data
	        .map(new MapFunction<String,Tuple2<Integer, String>>() {

				public Tuple2<Integer, String> map(String arg0) throws Exception {
					// TODO Auto-generated method stub
					return new Tuple2<Integer, String>(arg0.trim().length(), arg0);
				}
	            
	        })
	        .writeAsCsv("./data/outputFile");
	}
}
