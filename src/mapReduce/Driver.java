package mapReduce;

//reduce
//exec testing
//applications 1
//partitioning
//multi thread for rows in partition
//sorting for multiple intermediate files
//convert map to multiprocessing
//applications 2, 3
//fault tolerance

//if design issues done then implement applications to test the code




public class Driver{
	public void runner(){
	Master m = new Master("app.config");
	m.execute();
	
	}
	
}