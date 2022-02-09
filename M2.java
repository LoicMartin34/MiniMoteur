import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.openrdf.model.Value;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

//M2
public class M2 extends Engine{
	
	static List<ProjectionElem> variables_list;
	static HashMap<String, List<Integer>> results;
	
	public M2() {
		super();
	}
	
	public HashMap<String, List<Integer>> join(HashMap<String, List<Integer>> results, String join_variable, List<Integer> join_table, String new_variable, List<Integer> new_table){

		if(join_table.equals(results.get(join_variable))) {
			results.put(new_variable, new_table);
			return results;
		}
		
		List<Integer> end_table = new ArrayList<Integer>();
	
		for(int i = 0; i < results.get(join_variable).size(); i++) {
			boolean first = true;
			int val1 = results.get(join_variable).get(i);
			for(int y = 0; y < join_table.size(); y++) {
				int val2 = join_table.get(y);
				if(val1 == val2) {
					end_table.add(new_table.get(y));
					if(first) {
						first = false;
					}else{
						for(List<Integer> result: results.values()) {
							result.add(i, result.get(i));							
						}
						i++;
					}
				}
			}
			if(first) {
				for(List<Integer> result: results.values()) {
					result.remove(i);
				}
				i--;
			}
		}
		
		results.put(new_variable, end_table);
		
		return results;
	}
	
	public HashMap<String, List<Integer>> simpleJoin(HashMap<String, List<Integer>> results, String join_variable, Set<Integer> join_table){
		
		for(int i = 0; i < results.get(join_variable).size(); i++) {
			if(!join_table.contains(results.get(join_variable).get(i))) {
				for(List<Integer> result: results.values()) {
					result.remove(i);
				}
				if(i>=0) {i--;}
			}
		}
		
		return results;
	}		
}
