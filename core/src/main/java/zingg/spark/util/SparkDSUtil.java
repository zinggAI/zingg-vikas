package zingg.util;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import scala.collection.JavaConverters;
import zingg.client.Arguments;
import zingg.client.FieldDefinition;
import zingg.client.MatchType;
import zingg.client.SparkFrame;
import zingg.client.ZFrame;
import zingg.client.pipe.Pipe;
import zingg.client.util.ColName;
import zingg.client.util.ColValues;
import zingg.util.DSUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SparkDSUtil extends DSUtil<SparkSession, Dataset<Row>, Row, Column>{

    public static final Log LOG = LogFactory.getLog(SparkDSUtil.class);	

	

	public Column gt(ZFrame<Dataset<Row>, Row, Column> pairs, String c) {
		return pairs.col(ColName.ID_COL).gt(pairs.col(ColName.COL_PREFIX + ColName.ID_COL));
	}

	public Column equalTo(ZFrame<Dataset<Row>, Row, Column> a, String c, String e){
		return a.col(ColName.SOURCE_COL).equalTo(e);
	}

	public Column notEqual(ZFrame<Dataset<Row>, Row, Column> a, String c, String e) {
		return a.col(ColName.SOURCE_COL).notEqual(e);
	}

	

	
}
