package zingg.common.core.hash;


public class IsNullOrEmpty extends BaseHash<String,Boolean>{
	
	private static final long serialVersionUID = 1L;


	public IsNullOrEmpty() {
	    setName("isNullOrEmpty");
	}


	public Boolean call(String field) {
		 if (field == null || ((String ) field).trim().length() == 0) {
			 return null;
		 } else {
			 return true;
		 }
	}

}
