package zingg;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import zingg.client.Arguments;
import zingg.client.FieldDefinition;
import zingg.client.IZingg;
import zingg.client.MatchType;
import zingg.client.ZinggClientException;
import zingg.client.ZinggOptions;
import zingg.util.Analytics;
import zingg.util.BlockingTreeUtil;
import zingg.util.DSUtil;
import zingg.client.util.ListMap;
import zingg.util.Metric;
import zingg.feature.Feature;
import zingg.feature.FeatureFactory;
import zingg.hash.HashFunction;

import zingg.util.HashUtil;
import zingg.util.PipeUtilBase;

//Spark Session
//Dataset
//row
//column
public abstract class ZinggBase<T,D, R, C> implements Serializable, IZingg {

    protected Arguments args;
	
    protected T context;
    protected static String name;
    protected ZinggOptions zinggOptions;
    protected ListMap hashFunctions;
	protected Map<FieldDefinition, Feature> featurers;
    protected long startTime;
	public static final String hashFunctionFile = "hashFunctions.json";

    public static final Log LOG = LogFactory.getLog(ZinggBase.class);

    
    public abstract void init(Arguments args, String license)
        throws ZinggClientException;

    public abstract void setPipeUtil(PipeUtilBase<T,D,R,C> pipeUtil);
    public abstract void setDSUtil(DSUtil<T,D,R,C> pipeUtil);
    public abstract DSUtil<T,D,R,C> getDSUtil();

    public abstract PipeUtilBase<T,D,R,C> getPipeUtil();
   
    protected void initHashFns() throws ZinggClientException {
	}

    public void loadFeatures() throws ZinggClientException {
		try{
		LOG.info("Start reading internal configurations and functions");
		if (args.getFieldDefinition() != null) {
			featurers = new HashMap<FieldDefinition, Feature>();
			for (FieldDefinition def : args.getFieldDefinition()) {
				if (! (def.getMatchType() == null || def.getMatchType().equals(MatchType.DONT_USE))) {
					Feature fea = (Feature) FeatureFactory.get(def.getDataType());
					fea.init(def);
					featurers.put(def, fea);			
				}
			}
			LOG.info("Finished reading internal configurations and functions");
			}
		}
		catch(Throwable t) {
			LOG.warn("Unable to initialize internal configurations and functions");
			if (LOG.isDebugEnabled()) t.printStackTrace();
			throw new ZinggClientException("Unable to initialize internal configurations and functions");
		}
	}

    public void copyContext(ZinggBase<T,D, R, C> b) {
            this.args = b.args;
            this.featurers = b.featurers;
            this.hashFunctions = b.hashFunctions;
    }

	public void postMetrics() {
        boolean collectMetrics = args.getCollectMetrics();
        Analytics.track(Metric.EXEC_TIME, (System.currentTimeMillis() - startTime) / 1000, collectMetrics);
		Analytics.track(Metric.TOTAL_FIELDS_COUNT, args.getFieldDefinition().size(), collectMetrics);
        Analytics.track(Metric.MATCH_FIELDS_COUNT, getDSUtil().getFieldDefinitionFiltered(args, MatchType.DONT_USE).size(),
                collectMetrics);
		Analytics.track(Metric.DATA_FORMAT, getPipeUtil().getPipesAsString(args.getData()), collectMetrics);
		Analytics.track(Metric.OUTPUT_FORMAT, getPipeUtil().getPipesAsString(args.getOutput()), collectMetrics);

		Analytics.postEvent(zinggOptions.getValue(), collectMetrics);
	}

    public Arguments getArgs() {
        return this.args;
    }

    public void setArgs(Arguments args) {
        this.args = args;
    }

    public ListMap getHashFunctions() {
        return this.hashFunctions;
    }

    public void setHashFunctions(ListMap hashFunctions) {
        this.hashFunctions = hashFunctions;
    }

    public Map<FieldDefinition,Feature> getFeaturers() {
        return this.featurers;
    }

    public void setFeaturers(Map<FieldDefinition,Feature> featurers) {
        this.featurers = featurers;
    }

    
    public T getContext() {
        return this.context;
    }

    public void setContext(T spark) {
        this.context = spark;
    }
    public void setName(String name) {
        this.name = name;
    }
    public void setZinggOptions(ZinggOptions zinggOptions) {
        this.zinggOptions = zinggOptions;
    }

	public String getName() {
        return name;
    }

    public ZinggOptions getZinggOptions() {
        return zinggOptions;
    }

    public abstract HashUtil getHashUtil();
    public abstract BlockingTreeUtil getBlockingTreeUtil();


  
 }