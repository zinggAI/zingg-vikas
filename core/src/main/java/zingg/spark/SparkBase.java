package zingg.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;

import zingg.client.Arguments;
import zingg.client.FieldDefinition;
import zingg.client.IZingg;
import zingg.client.MatchType;
import zingg.client.ZinggClientException;
import zingg.client.ZinggOptions;
import zingg.util.Analytics;
import zingg.util.DSUtil;
import zingg.client.util.ListMap;
import zingg.util.Metric;
import zingg.feature.Feature;
import zingg.feature.FeatureFactory;
import zingg.hash.HashFunction;

import zingg.util.HashUtil;
import zingg.util.PipeUtil;

public abstract class SparkBase extends ZinggBase<SparkSession>{

    JavaSparkContext ctx;
    public static final Log LOG = LogFactory.getLog(SparkZinggBase.class);

    @Override
    public void init(Arguments args, String license)
        throws ZinggClientException {
        startTime = System.currentTimeMillis();
        this.args = args;
        try{
            context = SparkSession
                .builder()
                .appName("Zingg"+args.getJobId())
                .getOrCreate();
            ctx = new JavaSparkContext(context.sparkContext());
            JavaSparkContext.jarOfClass(IZingg.class);
            LOG.debug("Context " + ctx.toString());
            initHashFns();
            loadFeatures();
            ctx.setCheckpointDir("/tmp/checkpoint");	
        }
        catch(Throwable e) {
            if (LOG.isDebugEnabled()) e.printStackTrace();
            throw new ZinggClientException(e.getMessage());
        }
    }


    @Override
    public void cleanup() throws ZinggClientException {
        if (ctx != null) ctx.stop();
    }

   
    public void copyContext(ZinggBase<SparkSession> b) {
            super.copyContext(b);
            this.context = b.context;
    }

	
  
 }