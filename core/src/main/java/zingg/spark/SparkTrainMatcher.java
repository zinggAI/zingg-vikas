package zingg.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;

import zingg.TrainMatcher;
import zingg.client.Arguments;
import zingg.client.ZinggClientException;
import zingg.client.ZinggOptions;
import zingg.model.Model;
import zingg.spark.model.SparkModel;

/**
 * Spark specific implementation of TrainMatcher
 * 
 * @author vikasgupta
 *
 */
public class SparkTrainMatcher extends TrainMatcher<SparkSession, Dataset<Row>, Row, Column,DataType> {

	public static String name = "zingg.spark.SparkTrainMatcher";
	public static final Log LOG = LogFactory.getLog(SparkTrainMatcher.class);

	public SparkTrainMatcher() {
		setZinggOptions(ZinggOptions.TRAIN_MATCH);
		setContext(new ZinggSparkContext());
	}

    @Override
    public void init(Arguments args, String license)  throws ZinggClientException {
        super.init(args, license);
        getContext().init(license);
    }
        
	
	@Override
	protected Model getModel() {
		Model model = new SparkModel(getModelUtil().getFeaturers());
		model.register(getContext());
		model.load(args.getModel());
		return model;
	}


	@Override
	public void cleanup() throws ZinggClientException {
		// TODO Auto-generated method stub
		
	}


	
	

		
	
}
