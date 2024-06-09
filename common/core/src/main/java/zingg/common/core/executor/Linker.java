package zingg.common.core.executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import zingg.common.client.ZFrame;
import zingg.common.client.ZinggClientException;
import zingg.common.client.options.ZinggOptions;
import zingg.common.client.util.ColName;
import zingg.common.core.filter.PredictionFilter;
import zingg.common.core.match.output.IMatchOutputBuilder;
import zingg.common.core.match.output.LinkOutputBuilder;
import zingg.common.core.pairs.IPairBuilder;
import zingg.common.core.pairs.SelfPairBuilder;
import zingg.common.core.pairs.SelfPairBuilderSourceSensitive;



public abstract class Linker<S,D,R,C,T> extends Matcher<S,D,R,C,T> {

	private static final long serialVersionUID = 1L;
	protected static String name = "zingg.Linker";
	public static final Log LOG = LogFactory.getLog(Linker.class);

	public Linker() {
		setZinggOption(ZinggOptions.LINK);
	}
	
	@Override
	public ZFrame<D,R,C> selectColsFromBlocked(ZFrame<D,R,C> blocked) {
		return blocked;
	}

	@Override
	public IMatchOutputBuilder<S,D,R,C> getMatchOutputBuilder(){
		if (this.matchOutputBuilder == null) {
			this.matchOutputBuilder = new LinkOutputBuilder<S,D,R,C>(getDSUtil(), args);
		}
		return this.matchOutputBuilder;
	}

	@Override
	public IPairBuilder<S, D, R, C> getIPairBuilder(){
		if (this.iPairBuilder == null){
			iPairBuilder = new SelfPairBuilderSourceSensitive<S, D, R, C> (getDSUtil(),args);
		}
		return iPairBuilder;
	}
	
}
