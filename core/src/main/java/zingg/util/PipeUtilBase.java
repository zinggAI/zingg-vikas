package zingg.util;

import zingg.client.Arguments;
import zingg.client.pipe.Pipe;
//dataset
//spark session 
public interface PipeUtilBase<T, R> {
	

	public T readInternal(R spark, Pipe p, boolean addSource);

	public T readInternal(R spark, boolean addLineNo,
			boolean addSource, Pipe... pipes);

	public T read(R spark, boolean addLineNo, boolean addSource, Pipe... pipes);

	public T sample(R spark, Pipe p) ;

	public T read(R spark, boolean addLineNo, int numPartitions,
			boolean addSource, Pipe... pipes);

	public void write(T toWriteOrig, Arguments args, Pipe... pipes);

	public void writePerSource(T toWrite, Arguments args, Pipe[] pipes);

	public Pipe getTrainingDataUnmarkedPipe(Arguments args);

	public Pipe getTrainingDataMarkedPipe(Arguments args);
	
	public Pipe getModelDocumentationPipe(Arguments args);
	
	public Pipe getBlockingTreePipe(Arguments args);

	public String getPipesAsString(Pipe[] pipes);
}