package zingg.util;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.SparkSession;
import zingg.client.util.ListMap;
import zingg.hash.HashFnFromConf;
import zingg.hash.HashFunction;
import zingg.hash.HashFunctionRegistry;

import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.api.java.UDF1;


public interface HashUtil<D,R,C,T,T1> {
    /**
	 * Use only those functions which are defined in the conf
	 * All functions exist in the registry
	 * but we return only those which are specified in the conf
	 * @param fileName
	 * @return
	 * @throws Exception
	 */
	public ListMap<T1, HashFunction<D,R,C,T,T1>> getHashFunctionList(String fileName, Object spark)
			throws Exception ;
}
