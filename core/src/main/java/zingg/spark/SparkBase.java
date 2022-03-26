package zingg.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import zingg.ZinggBase;
import zingg.client.Arguments;
import zingg.client.IZingg;
import zingg.client.ZinggClientException;
import zingg.spark.util.SparkHashUtil;
import zingg.spark.util.SparkPipeUtil;
import zingg.util.BlockingTreeUtil;
import zingg.util.DSUtil;
import zingg.util.GraphUtil;
import zingg.util.HashUtil;
import zingg.util.PipeUtilBase;
import zingg.spark.util.SparkDSUtil;

public class SparkBase extends ZinggBase<SparkSession, Dataset<Row>, Row, Column>{

    JavaSparkContext ctx;
    public static final Log LOG = LogFactory.getLog(SparkBase.class);
    PipeUtilBase pipeUtil;
    HashUtil hashUtil;
    DSUtil dsUtil;
    GraphUtil graphUtil;
    BlockingTreeUtil blockingTreeUtil;

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
            setPipeUtil(new SparkPipeUtil(context));
            setDSUtil(new SparkDSUtil());
            setHashUtil(new SparkHashUtil());
            setGraphUtil(new GraphUtil());
            
        }
        catch(Throwable e) {
            if (LOG.isDebugEnabled()) e.printStackTrace();
            throw new ZinggClientException(e.getMessage());
        }
    }

    protected void initHashFns() throws ZinggClientException {
		try {
			//functions = Util.getFunctionList(this.functionFile);
			hashFunctions = getHashUtil().getHashFunctionList(hashFunctionFile, getContext());
		} catch (Exception e) {
			if (LOG.isDebugEnabled()) e.printStackTrace();
			throw new ZinggClientException("Unable to initialize base functions");
		}		
	}


    @Override
    public void cleanup() throws ZinggClientException {
        if (ctx != null) ctx.stop();
    }

   
    public void copyContext(ZinggBase<SparkSession, Dataset<Row>, Row, Column> b) {
            super.copyContext(b);
            this.context = b.getContext();
    }

    public HashUtil getHashUtil() {
        return hashUtil;
    }

    public void setHashUtil(HashUtil t) {
        hashUtil = t;
    }

    public GraphUtil getGraphUtil() {
        return graphUtil;
    }

    public void setGraphUtil(GraphUtil t) {
        graphUtil = t;
    }

    @Override
    public void execute() throws ZinggClientException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void setPipeUtil(PipeUtilBase<SparkSession, Dataset<Row>, Row, Column> pipeUtil) {
        this.pipeUtil = pipeUtil;
        
    }

    @Override
    public void setDSUtil(DSUtil<SparkSession, Dataset<Row>, Row, Column> pipeUtil) {
       this.dsUtil = pipeUtil;
        
    }

    @Override
    public DSUtil<SparkSession, Dataset<Row>, Row, Column> getDSUtil() {
        return dsUtil;
    }

    @Override
    public PipeUtilBase<SparkSession, Dataset<Row>, Row, Column> getPipeUtil() {
        return this.pipeUtil;
    }

    @Override
    public BlockingTreeUtil getBlockingTreeUtil() {
        return this.blockingTreeUtil;
    }


  
 }