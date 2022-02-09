import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.jena.query.* ;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

public class JenaSPARQL {
	
	static Model model;
	static ArrayList<HashMap<String, List<String>>> resultsMapList;
	
	public static void init(String path) {		 
			model = ModelFactory.createDefaultModel() ;			
			model.read(path) ;		
	}

	public static ArrayList<HashMap<String, List<String>>> run(List<String> queries) {
		
			resultsMapList = new ArrayList<HashMap<String, List<String>>>();
			
			for(String queryString: queries) {
				
				HashMap<String, List<String>> resultsMap = new HashMap<String, List<String>>();	
				
				Query query = QueryFactory.create(queryString) ;
				
				try (QueryExecution qexec = QueryExecutionFactory.create(query, model)) {
				    ResultSet results = qexec.execSelect() ;
				    
				    List<String> vars = results.getResultVars();
				    for(String var: vars) {
				    	resultsMap.put(var, new ArrayList<String>());
				    }
				    
				    for ( ; results.hasNext() ; )
				    {
				      QuerySolution soln = results.nextSolution() ;
				      for(String var: vars) {
				    	  RDFNode resource = soln.get(var);
				    	  if(resource != null) {
				    		  resultsMap.get(var).add(resource.toString());
				    	  }
				      }
				    }
				    resultsMapList.add(resultsMap);		
				  }
			
			}
			
			return resultsMapList;

	}

}
