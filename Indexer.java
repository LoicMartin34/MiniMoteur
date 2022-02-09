import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.openrdf.model.Statement;
import org.openrdf.model.Value;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;

public class Indexer {
	
	static HashMap<Integer, HashMap<Integer, Set<Integer>>> spo = new HashMap<Integer, HashMap<Integer, Set<Integer>>>();
	static HashMap<Integer, HashMap<Integer, Set<Integer>>> sop = new HashMap<Integer, HashMap<Integer, Set<Integer>>>();
	static HashMap<Integer, HashMap<Integer, Set<Integer>>> pso = new HashMap<Integer, HashMap<Integer, Set<Integer>>>();
	static HashMap<Integer, HashMap<Integer, Set<Integer>>> pos = new HashMap<Integer, HashMap<Integer, Set<Integer>>>();
	static HashMap<Integer, HashMap<Integer, Set<Integer>>> osp = new HashMap<Integer, HashMap<Integer, Set<Integer>>>();
	static HashMap<Integer, HashMap<Integer, Set<Integer>>> ops = new HashMap<Integer, HashMap<Integer, Set<Integer>>>();
	
	static HashMap<Integer, HashMap<Integer, Integer>> sp = new HashMap<Integer, HashMap<Integer, Integer>>();
	static HashMap<Integer, HashMap<Integer, Integer>> so = new HashMap<Integer, HashMap<Integer, Integer>>();
	static HashMap<Integer, HashMap<Integer, Integer>> ps = new HashMap<Integer, HashMap<Integer, Integer>>();
	static HashMap<Integer, HashMap<Integer, Integer>> op = new HashMap<Integer, HashMap<Integer, Integer>>();
	static HashMap<Integer, HashMap<Integer, Integer>> po = new HashMap<Integer, HashMap<Integer, Integer>>();
	static HashMap<Integer, HashMap<Integer, Integer>> os = new HashMap<Integer, HashMap<Integer, Integer>>();
	
	static HashMap<Integer, Integer> s_unary_index = new HashMap<Integer, Integer>();
	static HashMap<Integer, Integer> p_unary_index = new HashMap<Integer, Integer>();
	static HashMap<Integer, Integer> o_unary_index = new HashMap<Integer, Integer>();
	
	static int nb_index = 15;

	static HashMap<Value, Integer> dictionary = new HashMap<Value, Integer>();
	static List<Value> reverseDictionary = new ArrayList<Value>();
	
	static int indexValue = 0;
	static int stCount = 0;
	
	private static class DictionaryBuilder extends RDFHandlerBase {
		@Override
		public void handleStatement(Statement st) {
				
			if(!(dictionary.containsKey(st.getSubject()))) {
				dictionary.put(st.getSubject(), indexValue);
				reverseDictionary.add(st.getSubject());
				indexValue++;
			}
			if(!(dictionary.containsKey(st.getPredicate()))){
				dictionary.put(st.getPredicate(), indexValue);
				reverseDictionary.add(st.getPredicate());
				indexValue++;
			}
			if(!(dictionary.containsKey(st.getObject()))){
				dictionary.put(st.getObject(), indexValue);
				reverseDictionary.add(st.getObject());
				indexValue++;
			}
			
			if(Main.exportQueryStats) {
				stCount++;
			}
				
		}
	}
		
	private static class IndexBuilder extends RDFHandlerBase {
		@Override
		public void handleStatement(Statement st) {
					
			int s = dictionary.get(st.getSubject());
			int p = dictionary.get(st.getPredicate());
			int o = dictionary.get(st.getObject());
			
			loadIndex(spo, sp, s_unary_index, s, p, o);
			loadIndex(sop, so, s_unary_index, s, o ,p);
			loadIndex(pso, ps, p_unary_index, p, s, o);
			loadIndex(pos, po, p_unary_index, p, o ,s);
			loadIndex(osp, os, o_unary_index, o, s ,p);
			loadIndex(ops, op, o_unary_index, o, p ,s);	
		}
		
		private void loadIndex(HashMap<Integer, HashMap<Integer, Set<Integer>>> index, HashMap<Integer, HashMap<Integer, Integer>> binary_index, HashMap<Integer, Integer> unary_index, int r1, int r2, int r3) {
			if(!index.containsKey(r1)) {
				index.put(r1, new HashMap<Integer, Set<Integer>>());
				binary_index.put(r1, new HashMap<Integer, Integer>());
				unary_index.put(r1, 0);
			}
			if(!index.get(r1).containsKey(r2)) {
				index.get(r1).put(r2, new TreeSet<Integer>());
				binary_index.get(r1).put(r2, 0);
			}
			index.get(r1).get(r2).add(r3);
			unary_index.put(r1, unary_index.get(r1) + 1);
			binary_index.get(r1).put(r2, binary_index.get(r1).get(r2) + 1);
		}
	}


	public static void init(String path) throws FileNotFoundException {

		Reader reader = new FileReader(path);

		org.openrdf.rio.RDFParser rdfParser = Rio.createParser(RDFFormat.RDFXML);
		
		// creation du dictionnaire
		
		if(Main.exportQueryStats) {
			Main.stats.startTimeMeasure("creation dictionnaire");
		}
		
		rdfParser.setRDFHandler(new DictionaryBuilder());
		
		try {
	
			rdfParser.parse(reader, "");

//			Properties properties = new Properties();
//			for (Entry<Value, Integer> entry : dictionary.entrySet()) {
//			    properties.put(entry.getKey().toString(), entry.getValue().toString());
//			}
//			properties.store(new FileOutputStream("data.properties"), null);
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		if(Main.exportQueryStats) {
			Main.stats.pauseTimeMeasure("creation dictionnaire");
			Main.stats.saveMeasure("triples number", Integer.toString(stCount));
			Main.stats.startTimeMeasure("creation index");
		}
		
		try {
			reader.close();
		} catch(Exception e) {
			e.printStackTrace();
		}
		
		// creation de l'index
		
		rdfParser = Rio.createParser(RDFFormat.RDFXML);
		rdfParser.setRDFHandler(new IndexBuilder());
		
		try {
			reader = new FileReader(path);
			rdfParser.parse(reader, "");
			if(Main.exportQueryStats) {
				Main.stats.pauseTimeMeasure("creation index");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		try {
			reader.close();			
		} catch (IOException e) {
			e.printStackTrace();
		}	
		
	}

}
