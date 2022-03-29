package zingg.util;
import zingg.client.ZFrame;
import zingg.client.ZinggClientException;
import zingg.client.util.ColName;
import zingg.client.util.ColValues;
import zingg.model.Model;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public abstract class ModelUtil<S,D,R,C> {

    public static final Log LOG = LogFactory.getLog(ModelUtil.class);

	public Model<S,D,R,C> createModel(ZFrame<D,R,C> positives,
        ZFrame<D,R,C> negatives, Model<S,D,R,C> model, S spark) throws Exception, ZinggClientException {
        LOG.info("Learning similarity rules");
        ZFrame<D,R,C> posLabeledPointsWithLabel = positives.withColumn(ColName.MATCH_FLAG_COL, ColValues.MATCH_TYPE_MATCH);
        posLabeledPointsWithLabel = posLabeledPointsWithLabel.cache();
        posLabeledPointsWithLabel = posLabeledPointsWithLabel.drop(ColName.PREDICTION_COL);
        ZFrame<D,R,C> negLabeledPointsWithLabel = negatives.withColumn(ColName.MATCH_FLAG_COL, ColValues.MATCH_TYPE_NOT_A_MATCH);
        negLabeledPointsWithLabel = negLabeledPointsWithLabel.cache();
        negLabeledPointsWithLabel = negLabeledPointsWithLabel.drop(ColName.PREDICTION_COL);
        if (LOG.isDebugEnabled()) {
            LOG.debug(" +,-,Total labeled data "
                    + posLabeledPointsWithLabel.count() + ", "
                    + negLabeledPointsWithLabel.count());
        }
        model.register(spark);
        model.fit(posLabeledPointsWithLabel, negLabeledPointsWithLabel);
        return model;
    }



    
}
