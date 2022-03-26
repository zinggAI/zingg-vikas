package zingg.spark;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import org.apache.spark.sql.SparkSession;

import scala.collection.JavaConverters;
import zingg.Matcher;
import zingg.block.Block;
import zingg.block.BlockFunction;
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

public class SparkMatcher extends Matcher<SparkSession,Dataset<Row>,Row,Column>{


	protected static String name = "zingg.Matcher";
	public static final Log LOG = LogFactory.getLog(SparkMatcher.class);    

	protected ZFrame<Dataset<Row>,Row,Column> getBlockHashes(SparkFrame testData, Tree<Canopy<Row>> tree) {
		return testData.df().map(new Block.BlockFunction<Row>(tree), RowEncoder.apply(Block.appendHashCol(testData.df().schema())));
	}


    

		    


	@Override
	public void cleanup() throws ZinggClientException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void init(Arguments args, String license) throws ZinggClientException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPipeUtil(PipeUtilBase<SparkSession, Dataset<Row>, Row, Column> pipeUtil) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setDSUtil(DSUtil<SparkSession, Dataset<Row>, Row, Column> pipeUtil) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DSUtil<SparkSession, Dataset<Row>, Row, Column> getDSUtil() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PipeUtilBase<SparkSession, Dataset<Row>, Row, Column> getPipeUtil() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public HashUtil getHashUtil() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BlockingTreeUtil getBlockingTreeUtil() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public class SparkBlockFunction extends BlockFunction<Row> {
		
		public abstract Seq<Object> toSeq(R r) {

		}

		public Row createRow(List<Object> o){
			
		}


	}

}
