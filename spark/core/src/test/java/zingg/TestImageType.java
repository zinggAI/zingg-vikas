package zingg;

import java.util.Arrays;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import static org.apache.spark.sql.functions.callUDF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import zingg.common.core.similarity.function.ArrayDoubleSimilarityFunction;
import zingg.spark.core.executor.ZinggSparkTester;

public class TestImageType extends ZinggSparkTester{
	
	
	@Test
	public void testImageType() {
		
		Double[] d1 = {0.0,2.0};
		Double[] d2 = {0.0,1.0};
		Double[] d3 = {1.0,0.0};
		Double[] d4 = {-1.0,-1.0};
		Double[] d5 = {0.0,1.0,0.0};
		Double[] d6 = {1.0,0.0,0.0};
		Double[] d7 = {0.0,0.0,0.0};
		Double[] d8 = {1.0,0.0,0.0};
		
		
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d1, d2));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d2, d3));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d4, d3));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d5, d6));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d7, d8));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d8, d7));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d7, d7));
		System.out.println(ArrayDoubleSimilarityFunction.cosineSimilarity(d8, d8));
		
		System.out.println(DataTypes.createArrayType(DataTypes.DoubleType));
		System.out.println(DataTypes.createArrayType(DataTypes.StringType));
		System.out.println(DataType.fromJson("{\"type\":\"array\",\"elementType\":\"double\",\"containsNull\":true}"));
		System.out.println(DataType.fromJson("{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}"));
	}
	
	@Test
	public void testUDFArray() {
		
		try {
			// create a DF with double array as a column
			Dataset<Row> df = createSampleDataset();
			df.show();
			// check the schema of DF
			df.printSchema();
			// register ArrayDoubleSimilarityFunction as a UDF
			TestUDFDoubleArr testUDFDoubleArr = new TestUDFDoubleArr();
			spark.udf().register("testUDFDoubleArr", testUDFDoubleArr, DataTypes.DoubleType);
			// call the UDF from select clause of DF
			df = df.withColumn("cosine",
					callUDF("testUDFDoubleArr", df.col("image_embedding"), df.col("image_embedding")));
			// see if error is reproduced
			df.show();
			fail("exception expected");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

	@Test
	public void testUDFList() {
		
		try {
			// create a DF with double array as a column
			Dataset<Row> df = createSampleDataset();
			df.show();
			// check the schema of DF
			df.printSchema();
			
			// register ArrayDoubleSimilarityFunction as a UDF
			TestUDFDoubleList testUDFDoubleList = new TestUDFDoubleList();
			spark.udf().register("testUDFDoubleList", testUDFDoubleList, DataTypes.DoubleType);
			
			// call the UDF from select clause of DF
			df = df.withColumn("cosine", callUDF("testUDFDoubleList",df.col("image_embedding"),df.col("image_embedding")));
			// see if error is reproduced
			df.show();
			fail("exception expected");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
	}

	@Test
	public void testUDFSeq() {
		
		try {
			// create a DF with double array as a column
			Dataset<Row> df = createSampleDataset();
			df.show();
			// check the schema of DF
			df.printSchema();
			
			// register ArrayDoubleSimilarityFunction as a UDF
			TestUDFDoubleSeq testUDFDoubleSeq = new TestUDFDoubleSeq();
			spark.udf().register("testUDFDoubleSeq", testUDFDoubleSeq, DataTypes.DoubleType);
			
			// call the UDF from select clause of DF
			df = df.withColumn("cosine", callUDF("testUDFDoubleSeq",df.col("image_embedding"),df.col("image_embedding")));
			// see if error is reproduced
			df.show();
			fail("exception expected");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		
	}

	@Test
	public void testUDFWrappedArr() {
		
		// create a DF with double array as a column
		Dataset<Row> df = createSampleDataset();
		df.show();
		// check the schema of DF
		df.printSchema();
		
		// register ArrayDoubleSimilarityFunction as a UDF
		TestUDFDoubleWrappedArr testUDFDoubleWrappedArr = new TestUDFDoubleWrappedArr();
		spark.udf().register("testUDFDoubleWrappedArr", testUDFDoubleWrappedArr, DataTypes.DoubleType);
		
		// call the UDF from select clause of DF
		df = df.withColumn("cosine", callUDF("testUDFDoubleWrappedArr",df.col("image_embedding"),df.col("image_embedding")));
		// see if error is reproduced
		df.show();
		
		Row r = df.head();
		
		Double cos = (Double)r.getAs("cosine");
		double diff = 1-cos;
		System.out.println("cos diff "+diff+ " "+cos.getClass());
		
		assertTrue(diff<0.0000000001);
		
		
	}
	
	@Test
	public void testUDFObj() {
		
		// create a DF with double array as a column
		Dataset<Row> df = createSampleDataset();
		df.show();
		// check the schema of DF
		df.printSchema();
		
		// register ArrayDoubleSimilarityFunction as a UDF
		TestUDFDoubleObj testUDFDoubleObj = new TestUDFDoubleObj();
		spark.udf().register("testUDFDoubleObj", testUDFDoubleObj, DataTypes.DoubleType);
		
		// call the UDF from select clause of DF
		df = df.withColumn("cosine", callUDF("testUDFDoubleObj",df.col("image_embedding"),df.col("image_embedding")));
		// see if error is reproduced
		df.show();		
		
		Row r = df.head();
		
		Double cos = (Double)r.getAs("cosine");
		assertEquals(0.3, cos);
		System.out.println(""+cos+ " "+cos.getClass());

		
	}
	
	
	protected Dataset<Row> createSampleDataset() {
		
		StructType schemaOfSample = new StructType(new StructField[] {
				new StructField("recid", DataTypes.StringType, false, Metadata.empty()),
				new StructField("givenname", DataTypes.StringType, false, Metadata.empty()),
				new StructField("surname", DataTypes.StringType, false, Metadata.empty()),
				new StructField("suburb", DataTypes.StringType, false, Metadata.empty()),
				new StructField("postcode", DataTypes.StringType, false, Metadata.empty()),
				new StructField("image_embedding", DataTypes.createArrayType(DataTypes.DoubleType), false, Metadata.empty())		
		});

		
		Dataset<Row> sample = spark.createDataFrame(Arrays.asList(
				RowFactory.create("07317257", "erjc", "henson", "hendersonville", "2873g",new Double[]{0.1123,10.456,110.789}),
				RowFactory.create("03102490", "jhon", "kozak", "henders0nville", "28792",new Double[]{0.2123,20.456,220.789}),
				RowFactory.create("02890805", "david", "pisczek", "durham", "27717",new Double[]{0.3123,30.456,330.789}),
				RowFactory.create("04437063", "e5in", "bbrown", "greenville", "27858",new Double[]{0.4123,40.456,440.789}),
				RowFactory.create("03211564", "susan", "jones", "greenjboro", "274o7",new Double[]{0.5123,50.456,550.789}),
				RowFactory.create("04155808", "jerome", "wilkins", "battleborn", "2780g",new Double[]{0.6123,60.456,660.789}),
				RowFactory.create("05723231", "clarinw", "pastoreus", "elizabeth city", "27909",new Double[]{0.7123,70.456,770.789}),
				RowFactory.create("06087743", "william", "craven", "greenshoro", "27405",new Double[]{0.8123,80.456,880.789}),
				RowFactory.create("00538491", "marh", "jackdon", "greensboro", "27406",new Double[]{0.9123,90.456,990.789}),
				RowFactory.create("01306702", "vonnell", "palmer", "siler sity", "273q4",new Double[]{0.0123,100.456,11110.789})),
				schemaOfSample);

		return sample;
	}
	
	
	
	
}