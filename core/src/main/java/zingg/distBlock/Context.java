package zingg.distBlock;

import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Context {

    Dataset<Row> dataSample;
	// duplicates remaining after function is applied
	List<Row> matchingPairs;

    public Context(Dataset<Row> dataSample, List<Row> matchingPairs) {
        this.dataSample = dataSample;
        this.matchingPairs = matchingPairs;
    }


    public Dataset<Row> getDataSample() {
        return this.dataSample;
    }

    public void setDataSample(Dataset<Row> dataSample) {
        this.dataSample = dataSample;
    }

    public List<Row> getMatchingPairs() {
        return this.matchingPairs;
    }

    public void setMatchingPairs(List<Row> matchingPairs) {
        this.matchingPairs = matchingPairs;
    }

    public long getDataSampleSize() {
        if (dataSample == null) return 0;
        return dataSample.count();
    }

    public long getMatchingPairsSize() {
        if (matchingPairs == null) return 0;
        return matchingPairs.size();
    }

    
}
