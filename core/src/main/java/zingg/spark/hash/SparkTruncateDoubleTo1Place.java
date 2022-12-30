package zingg.spark.hash;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Spark specific trunc function for double (1 digit)
 * 
 * 
 * @author vikasgupta
 *
 */
public class SparkTruncateDoubleTo1Place extends SparkTruncateDouble {
	
	public static final Log LOG = LogFactory.getLog(SparkTruncateDoubleTo1Place.class);

	public SparkTruncateDoubleTo1Place(){
	    super(1);
	}
	
}
