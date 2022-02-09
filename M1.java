import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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


// M1
public class M1 extends Engine {
	
	static List<ProjectionElem> variables_list;
	static HashMap<String, List<Integer>> results;
	
	public M1() {
		super();
	}
	public HashMap<String, List<Integer>> join(HashMap<String, List<Integer>> results, String join_variable, List<Integer> join_table, String new_variable, List<Integer> new_table){
		
		if(join_table.equals(results.get(join_variable))) {
			results.put(new_variable, new_table);
			return results;
		}
		
		ArrayList<Integer> end_table = new ArrayList<Integer>();	
		List<Integer> keys = new ArrayList<Integer> (join_table);
		List<Integer> values = new ArrayList<Integer> (new_table);
		
		int position = 0;
		int ind = 0;	
		boolean first = true;
		
		while(position < results.get(join_variable).size()) {
			int key = keys.get(ind);
			int value = values.get(ind);
			if(results.get(join_variable).get(position) < key) {							
				if(first) {
					for(String k: results.keySet()) {
						results.get(k).remove(position);
					}					
				}else {
					position++;	
					first = true;
				}						
			}else if(results.get(join_variable).get(position) > key) {
				if(ind + 1 < keys.size()) {
					ind++;
					key = keys.get(ind);
					value = values.get(ind);
				}else {
					for(List<Integer> res: results.values()) {
						res.subList(position, res.size()).clear();
					}
					position = results.get(join_variable).size();
				}			
			}else {
				int start_ind = ind;
				while(position < results.get(join_variable).size() && results.get(join_variable).get(position) == key) {
					if(first) {
						first = false;
					}else {
						for(String k: results.keySet()) {
							results.get(k).add(position, results.get(k).get(position));
						}
						position++;
					}
					end_table.add(value);
					
					if(ind + 1 < keys.size() && key == keys.get(ind + 1)) {
						ind++;
						key = keys.get(ind);
						value = values.get(ind);
					}else if(position < results.get(join_variable).size()) {
						ind = start_ind;
						key = keys.get(ind);
						value = values.get(ind);
						first = true;
						position++;	
					}else {
						for(List<Integer> res: results.values()) {
							res.subList(position + 1, res.size()).clear();
						}
						key = -1;
						position = results.get(join_variable).size();
						break;						
					}
				}				
			}
		}

		results.put(new_variable, end_table);
		
		return results;
	}
	
	public HashMap<String, List<Integer>> simpleJoin(HashMap<String, List<Integer>> results, String join_variable, Set<Integer> join_table){
		
		ArrayList<Integer> new_list = new ArrayList<Integer>();
		
		int position = 0;
		
		Iterator<Integer> iterator = join_table.iterator();
		int key = iterator.next();
		
		while(iterator.hasNext() && position < results.get(join_variable).size()) {	
			//System.out.println(key + ",  " + results.get(join_variable).get(pos_gauche));
			if(results.get(join_variable).get(position) < key) {
				position++;
			}else if(results.get(join_variable).get(position) > key) {
				key = iterator.next();
			}else {
				new_list.add(key);
				position++;
				key = iterator.next();
			}
		};

		results.put(join_variable, new_list);
		
		return results;
	}
	
}
