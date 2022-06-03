package zingg.distBlock;

public class FnResult {

    // number of duplicates eliminated after function applied on fn context
	long elimCount = -1;
	// hash of canopy
	Object hash;


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


	@Override
	public String toString() {
		return "{" +
			" elimCount='" + getElimCount() + "'" +
			", hash='" + getHash() + "'" +
			"}";
	}


    
}
