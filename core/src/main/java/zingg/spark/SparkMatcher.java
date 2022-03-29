package zingg.spark;

import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import zingg.Matcher;
import zingg.block.Block;
import zingg.block.Canopy;
import zingg.block.Tree;
import zingg.model.Model;
import zingg.client.Arguments;
import zingg.client.SparkFrame;
import zingg.client.ZFrame;
import zingg.client.ZinggClientException;
import zingg.client.ZinggOptions;
import zingg.util.Analytics;
import zingg.util.BlockingTreeUtil;
import zingg.client.util.ColName;
import zingg.client.util.ColValues;
import zingg.util.Metric;
import zingg.client.util.Util;
import zingg.util.DSUtil;
import zingg.util.GraphUtil;
import zingg.util.HashUtil;
import zingg.util.ModelUtil;
import zingg.util.PipeUtilBase;

public class SparkMatcher extends Matcher<SparkSession,Dataset<Row>,Row,Column,DataType,DataType>{


	protected static String name = "zingg.Matcher";
	public static final Log LOG = LogFactory.getLog(SparkMatcher.class);    

	@Override
	protected ZFrame<Dataset<Row>,Row,Column> getBlockHashes(ZFrame<Dataset<Row>,Row,Column> testData, Tree<Canopy<Row>> tree) {
		Dataset<Row> retDF = testData.df().map(new SparkBlockFunction(tree), RowEncoder.apply(
			new Block<Dataset<Row>,Row,Column,DataType,DataType>().appendHashCol(testData.df().schema())));
		return new SparkFrame(retDF);

	}

	@Override
	public void cleanup() throws ZinggClientException {
		// TODO Auto-generated method stub
		
	}

}
