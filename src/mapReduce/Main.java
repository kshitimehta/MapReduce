package mapReduce;
class Main{
	public static void main(String[] args) {
		/*
		 * Invokes the Master class and starts the process
		 */
		System.out.println("Start..");
		Master m = new Master("app.config");
		m.execute();
		System.out.println("End...");
	}
}