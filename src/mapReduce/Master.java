package mapReduce;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.io.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.net.DatagramSocket;
import java.util.concurrent.atomic.AtomicBoolean;

class ActiveWorkersClass 
{ 
    // static variable single_instance of type Singleton 
    private static ActiveWorkersClass instance = null; 
  
    //HashMap to keep track of workers and processes.
    public HashMap<Integer,Process> activeWorkersMap; 
  
    // private constructor restricted to this class itself 
    private ActiveWorkersClass() 
    { 
    	activeWorkersMap = new HashMap<Integer,Process>(); 
    } 
  
    // static method to create instance of Singleton class 
    public static ActiveWorkersClass getInstance() 
    { 
        if (instance == null) 
            instance = new ActiveWorkersClass(); 
  
        return instance; 
    } 
} 

class InactiveWorkersClass 
{ 
    // static variable single_instance of type Singleton 
    private static InactiveWorkersClass instance = null; 
  
    // variable of type HashSet to keep track of inactive members either completed or failed. 
    public HashSet<Integer> inactiveWorkers; 
  
    // private constructor restricted to this class itself 
    private InactiveWorkersClass() 
    { 
    	 inactiveWorkers = new HashSet<Integer>(); 
    } 
  
    // static method to create instance of Singleton class 
    public static InactiveWorkersClass getInstance() 
    { 
        if (instance == null) 
            instance = new InactiveWorkersClass(); 
  
        return instance; 
    } 
}

class MapperStatus
{
    private static MapperStatus instance = null; 
    
    // variable of type HashMap to keep track of every mapper or reducer process and their error tags. 
    public HashMap<Integer,Integer> map; 
  
    // private constructor restricted to this class itself 
    private MapperStatus() 
    { 
    	 map = new HashMap<Integer,Integer>(); 
    } 
  
    // static method to create instance of Singleton class 
    public static MapperStatus getInstance() 
    { 
        if (instance == null) 
            instance = new MapperStatus(); 
  
        return instance; 
    } 
}

class CompleteClassCounter
{
    private static CompleteClassCounter instance = null; 
    
    // variable of type Integer to count the number of fully completed processes. 
    public Integer completeList; 
  
    // private constructor restricted to this class itself 
    private CompleteClassCounter() 
    { 
    	 completeList = 0; 
    } 
  
    // static method to create instance of Singleton class 
    public static CompleteClassCounter getInstance() 
    { 
        if (instance == null) 
            instance = new CompleteClassCounter(); 
  
        return instance; 
    } 
}

class KillClass
{
    private static KillClass instance = null; 
    
    // variable of type HashSet to handle  a 
    public HashSet<Process> killList; 
  
    // private constructor restricted to this class itself 
    private KillClass() 
    { 
    	killList = new HashSet<Process>(); 
    } 
  
    // static method to create instance of Singleton class 
    public static KillClass getInstance() 
    { 
        if (instance == null) 
            instance = new KillClass(); 
  
        return instance; 
    } 
	
}

class Master{
	
	// Initiate variables
    private static final int IOPort = 55001;
    ReentrantLock lock;
	Integer Num;
	Integer wIOPort;
    Integer wId;
    HashMap<Integer,Process> activeWorkersMap;
    HashSet<Integer> inactiveWorkersMap;
    HashSet<Process> processing;
    HashSet<String> complete;
    Queue<String> queue;
    
    String inputFile;
    String outputFileDir;
    String className;
    ServerSocket sSocket ;
    Socket socket ;
    BufferedReader bReader ;
    DataOutputStream outputStream ;
    boolean mapperFlag;
    
    public Master(String configFile) {
    	/*
    	 * Reads the config file and initializes the variables for the input, output files, number of workers and 
    	 * class name for the User Defined Function.
    	 */
    	lock  = new ReentrantLock();
    	Properties prop = new Properties();    
		InputStream is = null;
		try {
		    is = new FileInputStream("..\\"+configFile);
		} 
		catch (FileNotFoundException ex) {
			ex.getMessage();
		}
		try {
		    prop.load(is);
		} catch (IOException ex) {
		    
		}
		//creating a directory to store the intermediate files on disk
        File file = new File("tmp");		
        boolean bool = file.mkdir();
        if(bool){
           System.out.println("Directory created successfully");
        }else{
           System.out.println("Sorry couldn’t create specified directory");
        }
        
		inputFile = prop.getProperty("app.inputfilename");
		System.out.println(inputFile);
		outputFileDir = prop.getProperty("app.outputfilename");
		System.out.println(outputFileDir);
		Num = Integer.parseInt(prop.getProperty("app.N")); 
		className = prop.getProperty("app.class_name");
		//initializing the inactive worker class
        InactiveWorkersClass iWorkers = InactiveWorkersClass.getInstance();
    	for(int i =0;i<Num;i++) {
    		iWorkers.inactiveWorkers.add(i);
    	}
    	mapperFlag = false;    
    	
    }
    	
    
    public void execute() {
    	/* Main Execution function. 
    	 * Steps :
    	 * Compile Worker Code.
    	 * Initiate Mapper - Create Processes, pass them information.
    	 * Listen to Workers for completion information. If some workers die, find them in inactiveworkers 
    	 * and restart them.
    	 * If all complete, note it and start reducer and repeat same steps.
    	 * If not complete, print errors in the form of hashmap where key is id and value is error id.
    	 */
    	if(wId==null) {
    		wId = 0;
    	}
    	else {
    		wId += 1;
    	}
    	//Step1
    	mapper();
    	//Step 2
    	/*
    	try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		*/
    	while(true)
    	{
    		if(lock.tryLock())
    		{	
    			try 
    			{
	    			InactiveWorkersClass x = InactiveWorkersClass.getInstance();
	    			if(x.inactiveWorkers.size() >0) {
	    				if(x.inactiveWorkers.size()>this.Num) {
	    					System.out.println("Process Bomb chances. Exiting.");
	    					return;
	    				}
	    				HashSet<Integer> ids = new HashSet<Integer>(x.inactiveWorkers);
	    				for(Integer id: ids)
	    				{
	    					System.out.println("Restarting Worker:"+id);
	    					createWorker("mapper",id);
	    					x.inactiveWorkers.remove(id);
	    				}
	    			}
	    			else
	    			{
		    			ActiveWorkersClass aworker = ActiveWorkersClass.getInstance();
		    			if(aworker.activeWorkersMap.size()>this.Num) {
		    				System.out.println("Process Bomb happening.Exiting.");
		    				return;
		    			}
		    			
		    			else
		    			{			    			
		    				
		    				CompleteClassCounter count = CompleteClassCounter.getInstance();
		    				if(count.completeList != this.Num)
		    				{
		    					if( aworker.activeWorkersMap.size()==0){
		    					System.out.println("There exists an error in the code, address or files as given below. -1 : IO -2:FileNotFound -3:UDF related, -4,-5 reducer related.");
		    					MapperStatus status = MapperStatus.getInstance();
		    					System.out.println(status.map);
		    					return;
		    					}
		    				}
		    				else
		    				{
			    				System.out.println("I think all process were successfully completed.Resetting Values.");
		    					MapperStatus status = MapperStatus.getInstance();
		    					status.map = new HashMap<Integer,Integer>();
		    	    			InactiveWorkersClass iaworker = InactiveWorkersClass.getInstance();
		    	    			iaworker.inactiveWorkers = new HashSet<Integer>();
			    				CompleteClassCounter counter = CompleteClassCounter.getInstance();
			    				counter.completeList = 0;
			    				aworker.activeWorkersMap = new HashMap<Integer, Process>();
			    				break;
		    				}
		    			}
		    		}
    			}
    			finally {
    				lock.unlock();
    			}
    		}
    		else {
    			System.out.println("Unable to acquire lock");
    			try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    	
    	//while Set a counter here to check inactive and classCounter.
    	try {
			TimeUnit.SECONDS.sleep(5);
		} 
    	catch (InterruptedException e) 
    	{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	System.out.println("Entering reducer phase");
    	reducer();
    	try {
			Thread.sleep(5000);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}    	
    	while(true)
    	{	
    		if(lock.tryLock())
    		{
    			try 
    			{
	    			InactiveWorkersClass x = InactiveWorkersClass.getInstance();
	    			if(x.inactiveWorkers.size() >0) {
	    				if(x.inactiveWorkers.size()>this.Num) {
	    					System.out.println("Process Bomb chances. Exiting.");
	    					return;
	    				}
	    				
	    				HashSet<Integer> ids = new HashSet<Integer>(x.inactiveWorkers);
	    				for(Integer id: ids)
	    				{
	    					createWorker("reducer",id);
	    				}
	    				//System.out.println("Infinite Loop due to incomplete.");
	    			}
	    			else
	    			{
	    				
	    				
		    			ActiveWorkersClass aworker = ActiveWorkersClass.getInstance();
		    			if(aworker.activeWorkersMap.size()>this.Num) {
		    				System.out.println("Process Bomb happening.Exiting.");
		    				return;
		    			}
		    			else
		    			{	
		    				CompleteClassCounter count = CompleteClassCounter.getInstance();
		    				if(count.completeList != this.Num)
		    				{
		    					if(aworker.activeWorkersMap.size() ==0) {
		    					System.out.println("There exists an error in the code, address or files as given below. -1 : IO -2:FileNotFound -3:UDF related, -4,-5 reducer related.");
		    					MapperStatus status = MapperStatus.getInstance();
		    					System.out.println(status.map);
		    					return;
		    					}
		    				}
		    				else
		    				{
			    				System.out.println("I think all process were successfully completed.Resetting Values.");
		    					MapperStatus status = MapperStatus.getInstance();
		    					status.map = new HashMap<Integer,Integer>();
		    	    			InactiveWorkersClass iaworker = InactiveWorkersClass.getInstance();
		    	    			iaworker.inactiveWorkers = new HashSet<Integer>();
			    				CompleteClassCounter counter = CompleteClassCounter.getInstance();
			    				counter.completeList = 0;
			    				aworker.activeWorkersMap = new HashMap<Integer, Process>();
			    				break;
		    				}
		    			}
		    		}
    			}
    			finally {
    				lock.unlock();
    				
    			}
    		}
    		else {
    			System.out.println("Unable to acquire lock");
    			try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
    		}
    	}
    	
    	System.out.println("Done.");
    	
    }
    
    //private void killProcesses() {
    	//for(Process k: processing) {            
    		//k.destroyForcibly();
    	//}
    //}
    
    private void mapper() {
    	/*
    	 * Function to initiate worker processes with mapper flag.
    	 */
    	for(int i=0;i<Num;i++) {
    		createWorker("mapper",i);
    	}
    	return;
    }
    private void reducer() {
    	/*
    	 * Function to initiate worker processes with reducer flag.
    	 */
		InactiveWorkersClass iaworker = InactiveWorkersClass.getInstance();
		//iaworker.inactiveWorkers = new HashSet<Integer>();    	
    	for(int i=0;i<Num;i++) {
    		iaworker.inactiveWorkers.add(i);
    		createWorker("reducer",i);
    	}    	
    	return;
    }

    
    
	public static void main(String[] args){
			
		Master m = new Master("app.config");
		m.execute();
	}
	
    public static boolean checkAvailability(int port) {
    	/*
    	 * Function to confirm availability of a port.
    	 */
        ServerSocket s = null;
        DatagramSocket d = null;
        
    	if (port < 0 || port > 65535) {
    		return false;
        }
        try {
            s = new ServerSocket(port);
            s.setReuseAddress(true);
            d = new DatagramSocket(port);
            d.setReuseAddress(true);
            System.out.println("Sockets Working and Available.");
            return true;
        } catch (IOException e) {
        } finally {
            if (d != null) {
            	System.out.println("Closing Data");
                d.close();
            }
            if (s != null) {
                try {
                	System.out.println("Closing Server socket.");
                    s.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }	
    
	public void createWorker(String workType, Integer wId) {
		/*
		 * Function to initiate the workers and assign communication thread
		 * for the master for that worker with that port. The function also updates
		 *  all the signal and status data structures such as activeworker hashmaps and inactiveworker lists.
		 */
		
        ReentrantLock lock = new ReentrantLock();
        if (lock.tryLock()) {		
	        try {
	        	System.out.println("Starting Process building.");
	            //Starting Master Heartbeat
	            //lastHeartbeatPort = HEARTBEAT_PORT_START;
	        	this.wIOPort = IOPort;
	            while (!checkAvailability(this.wIOPort)) {
	            	this.wIOPort++;
	            }

	            System.out.println(this.wIOPort);
	            String filename=outputFileDir;
	            System.out.println(filename);
	            if(workType.equals("mapper")){
	            	filename = inputFile;
	            }
	            String[] command = new String[]{
	                    "java",
	                    "mapReduce.Worker",
	                    Integer.toString(wId),//replace with currentId.
	                    
	                    Integer.toString(wIOPort),
	                    workType,
	                    filename,
	                    Integer.toString(Num),
	                    this.className};
	            //ProcessBuilder is used to initialize multiple processes and run workers parallely in a distributed environment
	            ProcessBuilder processBuilder = new ProcessBuilder(command);
	            Process process = processBuilder.inheritIO().start();
	            
	            ActiveWorkersClass aWorkers = ActiveWorkersClass.getInstance();
	            aWorkers.activeWorkersMap.put(wId, process);
	            
	            InactiveWorkersClass iWorkers = InactiveWorkersClass.getInstance();
	            iWorkers.inactiveWorkers.remove(wId);
	            
	            MasterTaskComm  mtc = new MasterTaskComm(wId, this.wIOPort);
	            Thread MTC = new Thread(mtc);
	            MTC.start();
	            System.out.println("Worker Created and Executing " +  wId+"\n");
	            Thread.sleep(5);
	        } 
	        catch (Exception e) {
	            System.out.println(e.getStackTrace());
	        }
        }    
	}
}	

class MasterTaskComm implements Runnable
{
	
	Integer socketId;
	Integer wId;
    static ReentrantLock mainlock;
    private final AtomicBoolean running = new AtomicBoolean(false);	
	ServerSocket sSocket ;
	Socket socket ;
	BufferedReader bReader ;
	DataOutputStream oStream ;
	
	public MasterTaskComm(Integer workerId, Integer Port) {
		this.wId = workerId;
		this.socketId = Port;
		this.sSocket = null;
		this.socket = null;
		this.running.set(true);
		this.bReader = null;
	}
	
	public void stopThread() {
		this.running.set(false);
	}
	
	public void run() {
		/*
    	 * This method intiates the thread and socket for communication with the workers and runs the worker processes.
    	 */
		String data="";
		try {
			mainlock = new ReentrantLock();
			System.out.println("Initiating Server Sockets."+this.wId);
			try {
				this.sSocket = new ServerSocket(this.socketId);
				this.socket = sSocket.accept();
		        this.bReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			} catch(Exception e) {
				e.getMessage();
				data = "-6";
				System.out.println("S0ocket Errors000");
			}
			
			System.out.println("Initiation Done for Master Thread for worker:"+ this.wId);
	        data = null;
	    	while(this.running.get()) 
	    	{
	    		try {
	    			data = this.bReader.readLine();	
	    		}
		        catch(Exception e) {
		        	data = null;
		        	e.getMessage();
		        }
	    		if(data == null || !data.equals("WId"+this.wId.toString()))
	    		{
	    			System.out.println("Uncommon Data");
		    		if(data ==null || !data.equals("1"))
		    		{
		    			System.out.println("There is an error.");
		    			while(true) {
			            	if(mainlock.tryLock())
			            	{
				            	try {
				            	    ActiveWorkersClass aWorkers = ActiveWorkersClass.getInstance();
				    	            InactiveWorkersClass iWorkers = InactiveWorkersClass.getInstance();
				            		System.out.println("Error Status: "+data);
					            	System.out.println("Worker: "+this.wId.toString()+" is dead.");
					            	System.out.println("Worker being removed from active workers");
					            	MapperStatus x = MapperStatus.getInstance();
					            	
					            	synchronized(aWorkers.activeWorkersMap) {		            		
					            		aWorkers.activeWorkersMap.remove(this.wId);
						            	if(data == null)
						                {
						            		
						                    System.out.println(" Worker: "+ this.wId + " has heartbeat dead due to unknown reasons. Moving wId to Inactive processes to queue execution.\n");
							            	File[] Filex = new File("tmp\\").listFiles();

							            	for(File file: Filex) {
								            	System.out.println(file.getAbsolutePath());
							            		if(file.getName().contains('M'+this.wId.toString())) {
							            		System.out.println("Deleting: "+ file.getName());
							            		if (!file.delete()) {
							            			System.out.println("Unable to delete the intermediate files after worker failure....");
							            		}
							            		}
							            	}
							            	System.out.println("Deleted intermediate files if any existed.");
							            	iWorkers.inactiveWorkers.add(this.wId);
							            	this.stopThread();

						                }
						            	else {
							            	x.map.put(this.wId, Integer.parseInt(data));
							            	this.stopThread();
						            	}
						            	break;
					            	}
				            	}
				            	finally 
				            	{
				            		if(!mainlock.tryLock()) {
				            			mainlock.unlock();
				            		}
				            	}
			            	}
			            	else
			            	{
			            		Thread.sleep(30);
			            	}
			        	}
		        	System.out.println("Exiting Thread: "+this.wId.toString());    
		        }
		        else
		        {
		        	boolean flag = true;
		        	while(flag)
		        	{	
		                if(mainlock.tryLock())
		                {
		                    try
		                    {
			            	    ActiveWorkersClass aWorkers = ActiveWorkersClass.getInstance();
			            	    synchronized(aWorkers.activeWorkersMap) {
				            	    CompleteClassCounter x = CompleteClassCounter.getInstance();
				            	    if(x.completeList == null) 
				            	    {
				            	    	x.completeList = 1;
				            	    }
				            	    else 
				            	    {
				            	    x.completeList += 1;
				            	    }
				            	    aWorkers.activeWorkersMap.remove(this.wId);
			            	    } 
		                    }    
		                    finally {
		                        mainlock.unlock();
		                        flag=false;
		                    }
		                }
		        	}
		        	System.out.println("Worker successful: "+this.wId.toString()+" Exiting Thread. \n");
		        	Thread.sleep(20);
		        	this.stopThread();
		        	continue;          	
		        }    
	    	}
	    }	
	}
	catch(Exception e)
	{
		System.out.println("I am in catch Master");
		e.printStackTrace();
	}
	finally 
	{
		try {
			this.socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.sSocket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			this.bReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}			
		
	}
	}		
}
	

	
