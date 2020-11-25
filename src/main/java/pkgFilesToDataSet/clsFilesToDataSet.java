package pkgFilesToDataSet;

import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class clsFilesToDataSet {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
			SparkSession spark = SparkSession.builder()
			.appName("Read Data from a File")
			.master("local")
			.getOrCreate();

		Dataset<Row>  df = spark.read().format("csv")
				.option("header","true")
				.load("data/books.csv");
		
		df.show(5);
		
		df.printSchema();
	}

}
	
