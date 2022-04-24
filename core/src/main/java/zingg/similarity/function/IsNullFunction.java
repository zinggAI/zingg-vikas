package zingg.similarity.function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IsNullFunction extends BaseSimilarityFunction<String> {

	public static final Log LOG = LogFactory
			.getLog(IsNullFunction.class);

	public IsNullFunction() {
		super("IsNullFunction");
		// TODO Auto-generated constructor stub
	}

	public IsNullFunction(String name) {
		super(name);
	}


	@Override
	public Double call(String first, String second) {
		if (first != null && first.trim().length() !=0) return 1d;
		if (second != null && second.trim().length() !=0) return 1d;
		return 0d;		
	}

	

}
