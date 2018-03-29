// CS560 JT Liso, Sean Whalen
// Query the Inverted Index File

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.util.HashMap;
import java.util.Map;

public class Query {

	public Map<String, String> wordLocations;
	
	public Query() {
		wordLocations = new HashMap<String,String>();
		return;
	}
	
	public void readFile(String fname) throws IOException {
		FileReader fr = new FileReader(fname);
		BufferedReader br = new BufferedReader(fr);
		
		String line = null;
		String kv[];
		//int space_errors = 0;
		int i = 0;
		
		while ( (line = br.readLine()) != null) {
			line = line.replaceAll("\\s+", " "); //collapse spaces
			kv = line.split(" ");
			
			//if weird amount of spaces skip line
			if (kv.length != 2) {
				//space_errors++;
				continue;
			}
			
			//add to map if enough memory
			try {
				wordLocations.put(kv[0],kv[1]);
			}
			catch (Exception e) {
				System.err.println(e);
			}
			
		}
		
		//System.err.println(space_errors);
		br.close();
		return;
	}

	public void queryLoop() {
		Scanner console = new Scanner(System.in);
		String query;
		
		while (console.hasNextLine()) {
			query = console.nextLine();
			query = query.replaceAll("\n", "");
			String res = null;
			res = wordLocations.get(query);
			if (res != null) {
				System.out.println(res);
				System.out.println();
			}
		}
		
		return;
	}

	public static void main(String args[]) {
		if (args.length != 1) {
			return;
		}
		Query Q = new Query();
		
		try {
			Q.readFile(args[0]);
		}
		catch (IOException e) {
			System.err.println(e);
			return;
		}
		
		Q.queryLoop();
		return;
	}
}
