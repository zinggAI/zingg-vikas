package zingg.model;

import java.io.IOException;
import java.util.Map;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import zingg.client.FieldDefinition;
import zingg.feature.Feature;
import zingg.client.util.ColName;

public abstract class LabelModel<S,D,R,C> extends Model<S,D,R,C>{
	
	public LabelModel(Map<FieldDefinition, Feature> f) {
		super(f);
		// TODO Auto-generated constructor stub
	}

	

}
