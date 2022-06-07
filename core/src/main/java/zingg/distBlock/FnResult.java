package zingg.distBlock;

import java.io.Serializable;

public class FnResult implements Comparable<FnResult>, Serializable {

    // number of duplicates eliminated after function applied on fn context
	long elimCount = -1;
	// hash which resulted in this canopy
	Object hash;
	long approxChildren;
	
	public FnResult() {

	}

	public FnResult(long elimCount, Object hash, long approxChildren) {
		this.elimCount = elimCount;
		this.hash = hash;
		this.approxChildren = approxChildren;
	}



	public long getElimCount() {
		return this.elimCount;
	}

	public void setElimCount(long elimCount) {
		this.elimCount = elimCount;
	}

	public Object getHash() {
		return this.hash;
	}

	public void setHash(Object hash) {
		this.hash = hash;
	}

	public int compareTo(FnResult other) {
		return (this.elimCount >= other.elimCount ? -1 : 1);
	}



	@Override
	public String toString() {
		return "{" +
			" elimCount='" + getElimCount() + "'" +
			", hash='" + getHash() + "'" +
			", children='" + approxChildren + "'" +
			"}";
	}
    
}
