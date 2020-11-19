package mapReduce;
import java.io.*;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.lang.reflect.*;
import java.lang.Math;
import java.util.concurrent.atomic.AtomicBoolean;

class ProcessStatus
{
    private static ProcessStatus instance = null; 
    
    // variable of type String 
    public Integer status; 
  
    // private constructor restricted to this class itself 
    private ProcessStatus() 
    { 
    	 status = null; 
    } 
  
    // static method to create instance of Singleton class 
    public static ProcessStatus getInstance() 
    { 
        if (instance == null) 
            instance = new ProcessStatus(); 
  
        return instance; 
    } 
}



public class Worker{

    Integer workerId;
    Integer mainSocketValue;
    Socket wSocket;
    DataOutputStream wOutputStream;
    BufferedReader wReader;
    String workType;
    String inputFile;
    Integer totalWorkers;
    Class cls;
    String className;
    static ReentrantLock mainlock;    
    private static final String OUTPUT_DIR = "tmp\\";
    
    public Worker(Integer WorkerId, Integer MainSocket, String WorkType, String InputFile, Integer Num, String ClassName)
    {
    	/*
    	 initializes a worker id, socket value, class name, input file and total workers.
    	*/
        workerId = WorkerId;
        mainSocketValue = MainSocket;
        wSocket = null;
        wOutputStream = null;
        wReader = null;
        workType = WorkType;
        inputFile = InputFile;
        totalWorkers = Num;
        className = ClassName;
        UDF1 x = new UDF1();
        cls = x.getClass();
    }
    public Worker(Integer MainSocket) {
        mainSocketValue = MainSocket;    	
    }
    public void getSocket() throws IOException
    {
    	wSocket = new Socket("localhost", this.mainSocketValue);
        wOutputStream = new DataOutputStream(wSocket.getOutputStream());
        wReader = new BufferedReader(new InputStreamReader(wSocket.getInputStream()));
    }  
    public void closeSocket() throws IOException
    {
        if(wSocket != null) {
        	wSocket.close();
        }
        if(wOutputStream != null) {
            wOutputStream.close();
        }
        if(wReader != null) {
            wReader.close();
        }
    }    
    
    private HashMap<Integer, String> cleanData(Integer pointer, Integer partitionSize, boolean lastFlag) throws FileNotFoundException, IOException{
    	/*
    	 * This functions takes as input the pointer from where the worker has to start scanning, the partition size and 
    	 * a flag to indicate if the limit for the total number of workers has reached.
    	 */
    	/*
    	 * We use a random access file object to access the file from the specified pointer
    	 * and avoid scanning the entire file
    	 */
    	HashMap<Integer,String> result = new HashMap<Integer, String>();
    	RandomAccessFile raf = null;
    	String s = "";
		File f = new File(inputFile);
		Integer file_size = (int) f.length();
    	raf = new RandomAccessFile(this.inputFile,"r");
    	raf.seek(pointer);
    	if(lastFlag == true) {
    		/*
    		 * if we are on the last worker then the worker will read the rest of the file
    		 */
    		byte[] b = new byte[file_size-pointer];
    		raf.read(b, 0, b.length);
    		s += new String(b);		
    	}
    	else {
    		/*
    		 * the worker will read only the partition size and not more
    		 */
    		char k;
		    byte[] b = new byte[partitionSize];
    		raf.read(b, 0, b.length);			    
		    s += new String(b);
		    k = s.charAt(s.length()-1);
		    if(k != '\n')
		    {
		    	/*
		    	 * if the partition does not end at the next line then the worker will store the extra bytes till 
		    	 * it reaches the next line
		    	 */
	    		String extra = raf.readLine();   
	    		s+= extra;
    		}
    	}
    	/*
    	 * Split by next line
    	 */
    	String[] rows = s.split("[\\r\\n]+");
		for(int i=0;i< rows.length;i++) {
			result.put(i, rows[i]);
		}    
		
    	if(pointer !=0)
    	{
    		/*
    		 * worker will make sure it starts reading with a new line
    		 */
    		raf.seek(pointer-1);
    		if(raf.readChar()!='\n')
    		{
    			result.remove(0);
    		}
    		
    	}
   
	    try {
				raf.close();
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	    return result;

    }
   	
    
    private ArrayList<Map.Entry<Object,Object>> invokeUDF(HashMap<Integer, String> data) throws NoSuchMethodException,IllegalAccessException,InvocationTargetException, ClassNotFoundException, InstantiationException {

    	/*
    	 * This function is used to invoke the map function from the User Defined Class using the reflection library
    	 */
    	ArrayList<Map.Entry<Object,Object>> keyList = new ArrayList<Map.Entry<Object, Object>>();
    	System.out.println("Invoking UDF");
        
    	Object output = null;
		if(data != null) {
		      for (HashMap.Entry<Integer, String> entry : data.entrySet()) 
		      {
		    	 /*
		    	  * create a class object of the UDF class name passed from the master 
		    	  */
		    	Class<?> user_class = Class.forName("mapReduce."+this.className);
		  		Object user;
		  		user = user_class.newInstance();
		  		/*
		  		 * create arguments to pass to the UDF class and method object for the map function 
		  		 */
		        Class[] cArg = new Class[2];
		        cArg[0] = Integer.class;
		        cArg[1] = String.class;
		        Method test_func = null;
				test_func = user.getClass().getMethod("map_func", cArg);
				Object arglist[] = new Object[2];
				arglist[0] = new Integer(entry.getKey());
				arglist[1]= new String(entry.getValue());

//				invoke the map function from the UDF class with the arguments as key, value pairs
				output = test_func.invoke(user,arglist);		        	
		        HashMap<Object,Object> outputNew = (HashMap<Object,Object>) output;
//		        dump the contents from the map function into a HashMap of objects to generalize the use
				for (HashMap.Entry<Object, Object> keys : outputNew.entrySet()) 
				{
					  keyList.add(keys);					
				}
		      }
		}      
    	return keyList;
    }

	private Integer intFileWriter(ArrayList<Map.Entry<Object, Object>> mappedData, Integer N, Integer workerId){
			
		/*
		 * This function is used to write the output from the map function to the intermediate files on disk
		 */
		System.out.println("Writing to intermediate files.");
		FileWriter fstream = null;
		BufferedWriter out = null;
		
		for(Map.Entry<Object, Object> keys:mappedData) {
			/*
			 * By using hashcode we ensure uniqueness of the keys in our output from the map function
			 * We create R intermediate files which map to each reducer for a single mapper
			 * The total intermediate files created are N*N, each for a mapper and reducer pair
			 */
			Integer hashVal = keys.getKey().hashCode();
			Integer R = Math.abs(hashVal)%N;
			String outputfilename = OUTPUT_DIR+"M"+this.workerId.toString()+"_"+"R"+R.toString()+".txt";
			
			try {
				fstream = new FileWriter(outputfilename, true);
			} catch (IOException e1) {
				e1.printStackTrace();
				return -1;
			}
			out = new BufferedWriter(fstream);
			try {
				//System.out.println()
				out.write(keys.getKey().toString() + "\t" + keys.getValue().toString());
				out.write("\n");
				out.flush();
			}
			catch(IOException e) {
				System.out.println(e.getMessage());
				return -1;
			}
		}
		try {
			fstream.close();
			out.close();
		}
		catch(IOException e) {
			System.out.println(e.getMessage());
			return -1;
		}
		System.out.println("Files Written.");
		return 1;
	}
    
    private Integer map() throws IOException { 
    	/*
    	 * This function is used to invoke all the functions to clean the Data, read the input file by partitions,
    	 * invoke the UDF functions and write the output to the N*N intermediate files
    	 */
	    	System.out.println("Entered Mapping Function");
	    	System.out.println(inputFile);
			File f = new File(inputFile);
			Integer file_size = (int) f.length();
			System.out.println("FileSize: ");
			System.out.println(file_size);		
			Integer partitionSize = (file_size/(this.totalWorkers));	
			System.out.println("Partition Size: ");
			System.out.println(partitionSize.toString());
			Integer pointer = partitionSize*workerId;
			System.out.println("Worker Id");
			System.out.println(workerId);
			boolean lastFlag = false;
			HashMap<Integer, String> data = null;
			if(workerId == totalWorkers-1) 
			{
				lastFlag = true;
			}
			try 
			{
			data = cleanData( pointer, partitionSize, lastFlag);
			}
			catch(FileNotFoundException e) 
			{
				return -2;
			}
			catch(IOException e) 
			{
				return -1;
			}
			System.out.println("Data read and cleaned.");
			ArrayList<Map.Entry<Object, Object>> keyList = null;
			try 
			{
				try 
				{
					System.out.println("Invoking UDF map function....");
					keyList = invokeUDF(data);
				} catch (ClassNotFoundException | InstantiationException e) 
				{
					// TODO Auto-generated catch block
					e.printStackTrace();
					return -3;					

				}
			} 
			catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) 
			{
				// TODO Auto-generated catch block
				return -3;					
			}
			
			Integer result = intFileWriter(keyList, totalWorkers, workerId);
			System.out.println("Mapper Stage Completed.");
			return result;
    }

	public Map<String, ArrayList<String>> reducerRead(String[] pathnames) throws NumberFormatException, IOException,FileNotFoundException 
	{	
		/*
		 * This functions is used for the reducer workers to read the intermediate files on the disk and generate an ArrayList of all values for
		 * a the same key. This Hashmap is then passed to the UDF reduce function to combine and return. 
		 */
		System.out.println("Reading Intermediate Data: "+this.workerId);
		String line;
		String key;
		
		HashMap<String, ArrayList<String>> notsortedMap = new HashMap<>();	
		//System.out.println("Number of pathnames:"+pathnames.length+" "+this.workerId);
		for(String path: pathnames) 
		{
			
			String[] split1 = path.split("_R");
			Integer c = 0;
			while(split1[1].charAt(c)!='.') 
			{
				c++;
			}
			String yx =split1[1].substring(0, c);
			if(yx.equals(this.workerId.toString())) 
			{	
				System.out.println("File and worker: "+this.OUTPUT_DIR+path+"        "+this.workerId);
				FileReader f = new FileReader(this.OUTPUT_DIR+path);
				BufferedReader br = new BufferedReader(f);
				while((line=br.readLine()) != null)
				{	
					//System.out.println(line);
					String[] pairs = line.split("\t");
					key = pairs[0];
					ArrayList<String> value1 = notsortedMap.getOrDefault(key, new ArrayList<String>());
					value1.add(pairs[1]);
					notsortedMap.put(key, value1);			
				}
		    }
		}	
		System.out.println("Completed Reading the intermediate files...");
		//System.out.println(notsortedMap);
		return notsortedMap;
	}
	
	private HashMap<String,String> invokeReducer(Map<String, ArrayList<String>> output) throws NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, ClassNotFoundException, InstantiationException{
		
		/*
		 * This function is used to invoke the reduce function by the User by creating a class instance and using the reflection library to 
		 * invoke it with the arguments.
		 */
		System.out.println("Invoking reduce functions.");
		Object outputx = null;
		HashMap<String,String> totalOutput = new HashMap<String,String>();
		for(Map.Entry<String, ArrayList<String>> entry : output.entrySet())
		{	
			/*
	    	  * create a class object of the UDF class name passed from the master 
	    	  */
			Class<?> user_class = Class.forName("mapReduce."+this.className);
	  		Object user;
	  		user = user_class.newInstance();
	  		/*
	  		 * create arguments to pass to the UDF class and method object for the map function 
	  		 */
	        Class[] cArg = new Class[2];
	        cArg[0] = String.class;
	        cArg[1] = ArrayList.class;
	        Method test_func = null;
			test_func = user.getClass().getMethod("reduce_func", cArg);
		
			Object arglist[] = new Object[2];
			arglist[0] = new String(entry.getKey());
			arglist[1]= new ArrayList<String>(entry.getValue());
			
//			invoke the map function from the UDF class with the arguments as key, value pairs
	        outputx = test_func.invoke(user,arglist);
//	        dump the contents from the map function into a HashMap of string and objects to generalize the use
			HashMap<String,Object> bOutput = (HashMap<String,Object>) outputx;
	        for(Map.Entry<String, Object> kv : bOutput.entrySet()) {
	        	totalOutput.put(kv.getKey().toString(), kv.getValue().toString());
	        }
		    
		}
		
		return totalOutput;
	}
	private Integer redFileWriter(HashMap<String,String> sortedMap) throws IOException {
		/*
		 * This function is used to write the output from the reduce function to the output file directory specified in the config file
		 */
		System.out.println("Writing output data.");
		String outname = this.inputFile+this.workerId+".txt";
		System.out.println(outname);
		FileWriter fstream = null;
		try {
			fstream = new FileWriter(outname);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		BufferedWriter out = new BufferedWriter(fstream);
		for (Map.Entry<String, String> entry : sortedMap.entrySet())
		{
			try {
				out.write(entry.getKey() + "\t" + entry.getValue());
				out.write("\n");
			    out.flush();
			}
			catch(Exception e) {
				e.getMessage();
				return -4;			
			}
		}
		out.close();
		return 1;
	}		
		
	
    private Integer reducer() {
    	/*
    	 * This function is used to call the functions for reduce worker, UDF and file writer for reduce worker
    	 */
    	String[] pathnames = null;
    	File f = null;
		try {
			f = new File(OUTPUT_DIR);
			pathnames = f.list();

		}
		catch(Exception e)
		{
			e.getMessage();
			return -2;
		}
		
		Map<String, ArrayList<String>> output;
		HashMap<String, String> finalOutput = null;		
		Integer r = null;
    	try {
			output = reducerRead(pathnames);
		} catch (NumberFormatException e1) {
			e1.printStackTrace();
			return -5;
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -2;
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return -1;
		}
		try {
			finalOutput = invokeReducer(output);
		} catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
				| InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -3;
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -3;
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
		try {
			r = redFileWriter(finalOutput);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
    	return r;
    }
    public void execute() 
    {	
    	/*
    	 * This method is used to execute the worker jobs and use locks so that the workers do not interfere.
    	 * We initialize threads for workers to communicate with the master with the corresponding worker id and socket value.
    	 * The master checks socket value to monitor the activity of the worker.
    	 */
    	mainlock = new ReentrantLock();
    	System.out.println("Starting Execution: WorkerId"+this.workerId.toString());
    	System.out.println(this.workType);
    	
    	WorkerTaskComm wtc = new WorkerTaskComm(this.workerId, this.mainSocketValue);
        Thread WTC = new Thread(wtc);
        WTC.start();   
        try {
			Thread.sleep(100);
		} catch (InterruptedException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
    	if(this.workType.equals("mapper"))
    	{
    			try {
					Integer r = map();
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					while(true) {
						if(mainlock.tryLock())
						{
							try{
								ProcessStatus p = ProcessStatus.getInstance();
								p.status = r;
								System.out.println(r);
							}
							finally {
								mainlock.unlock();
							}
							break;
						}
						try {
							Thread.sleep(5000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					while(true) {
						if(mainlock.tryLock())
						{	Integer i = null;
							try{
								ProcessStatus p = ProcessStatus.getInstance();
								i = p.status;
							}
							finally {
								mainlock.unlock();
							}
							if(i==100) {
							break;
							}
						}	
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
    			}	
					catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    			System.out.println("Exiting Map Process.");
    	}
    	else {
			try {
				Integer r = reducer();
				while(true) {
					if(mainlock.tryLock())
					{
						try{
							ProcessStatus p = ProcessStatus.getInstance();
							p.status = r;
						}
						finally {
							mainlock.unlock();
						}
						break;
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				while(true) {
					if(mainlock.tryLock())
					{	Integer i = null;
						try{
							ProcessStatus p = ProcessStatus.getInstance();
							i = p.status;
						}
						finally {
							mainlock.unlock();
						}
						if(i==100) {
						break;
						}
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}				
			}
			finally
			{
				//System.out.println("Process Ended.");
			}
		}
    }
    
    public static void main(String[] args){
    	/*
    	 * This is the main method that creates the worker instances and calls them
    	 */
    	try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println("Initiating");
        if(args.length < 3) {
            System.out.println("Worker: Insufficient Arguments\n");
            return;
        }
        Worker w = new Worker(Integer.parseInt(args[0]), Integer.parseInt(args[1]), args[2], args[3], Integer.parseInt(args[4]),args[5]);
        System.out.println("Workers Commence.");
        w.execute();
        System.out.println("Exiting Worker Process");
    }
}


class WorkerTaskComm implements Runnable{
	
    DataOutputStream wOutputStream;
    BufferedReader wReader;
    Integer mainSocket;
    Socket wSocket;
    Integer wId;
    static ReentrantLock lock;
    private final AtomicBoolean running = new AtomicBoolean(false);	//static ReentrantLock lock;

    public WorkerTaskComm(Integer WId, Integer MainSocket) {
    	this.mainSocket = MainSocket;
    	this.wSocket = null;
    	this.wId = WId;
    	this.running.set(true);
    }
    
    public void stopThread() {
    	this.running.set(false);
    }
    
    public void run()
    {	
    	/*
    	 * This method intiates the thread and socket for communication with the master and runs the worker processes.
    	 */
    	lock = new ReentrantLock();
    	System.out.println("INITIATING THREAD. "+this.wId);
    	System.out.println("Socket"+mainSocket.toString());
    	try {
			this.wSocket = new Socket("localhost",this.mainSocket);
	    	this.wOutputStream = new DataOutputStream(this.wSocket.getOutputStream());
	    	//System.out.println("Socket Acquired.");
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println("Initiation Done "+this.wId);
    	try 
    	{
	    	while(this.running.get()) 
	    	{
	    		Integer r = 0;
	    		if(lock.tryLock())
	    		{
		    		try {
		    			ProcessStatus p = ProcessStatus.getInstance();
		    			r = p.status;
		    			if(r!=null)
		    				p.status=100;
		    			}
						finally 
						{
							lock.unlock();
						}
					if(r==null) 
					{	
						
						try {
							this.wOutputStream.writeBytes("WId"+this.wId.toString()+"\n");
							try {
								Thread.sleep(3000);
							}
							catch (InterruptedException e) 
							{
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						} 
						catch (IOException e) 
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}	
					}
					else
					{
						try {
							
							this.wOutputStream.writeBytes(r.toString()+"\n");
							this.stopThread();
						} 
						catch (IOException e) 
						{
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
	    		}
		    	else 
		    	{
		    		try 
					{
						this.wOutputStream.writeBytes("WId"+this.wId.toString()+"\n");
					} 
					catch (IOException e1) 
					{
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
					try 
					{
						Thread.sleep(200);
					} 
					catch (InterruptedException e) 
					{
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
		    	}	
		    }
    	}
    	
    catch(Exception e)
    	{
    	System.out.println("I am in the catch");
    	e.getMessage();
    	}
    	finally {
			try {
				this.wSocket.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				this.wOutputStream.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			    		
    	}
    	System.out.println("Exiting Thread.");
    	
    			
    	
    }
}