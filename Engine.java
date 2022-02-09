import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.algebra.Projection;
import org.openrdf.query.algebra.ProjectionElem;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

public abstract class Engine {
	
	SPARQLParser sparqlParser;
	List<ProjectionElem> variables_list;
	List<HashMap<String, List<Integer>>> all_results;
	
	List<HashMap<String, List<Integer>>> temp_results;
	
	public abstract HashMap<String, List<Integer>> join(HashMap<String, List<Integer>> results, String join_variable, List<Integer> join_table, String new_variable, List<Integer> new_table);
	public abstract HashMap<String, List<Integer>> simpleJoin(HashMap<String, List<Integer>> results, String name, Set<Integer> hashSet);
	
	public Engine() {
		sparqlParser = new SPARQLParser();
		all_results = new ArrayList<HashMap<String, List<Integer>>>();
	}
	
	public HashMap<String, List<String>> processQuery(String query) {
		
		HashMap<String, List<Integer>> results = new HashMap<String, List<Integer>>();
		
		List<String> variables = new ArrayList<String>();					
		
		ParsedQuery pq;
		List<StatementPattern> patterns = null;
		
		try {
			pq = sparqlParser.parseQuery(query, null);
			patterns = StatementPatternCollector.process(pq.getTupleExpr());
			
			pq.getTupleExpr().visit(new QueryModelVisitorBase<RuntimeException>() {
				public void meet(Projection projection) {
					variables_list = projection.getProjectionElemList().getElements();
				}
			});
			
			for(ProjectionElem v: variables_list) {
				variables.add(v.getSourceName());
			}
		} catch (MalformedQueryException e1) {
			System.out.println("Query mal formée");
			e1.printStackTrace();
			return null;
		}
		
//		if(Main.optimizeQuery && patterns.size() > 1) {
//			List<StatementPattern> optimizedPatterns = optimizeQuery(patterns);
//			patterns = optimizedPatterns;
//		}	
		
		try {
				
			for(StatementPattern sp: patterns) {
					
					Var s = sp.getSubjectVar();
					Var p = sp.getPredicateVar();
					Var o = sp.getObjectVar();						
					
					 int s_index, o_index;
					
					 int p_index = Indexer.dictionary.get(p.getValue());
					 
//					 System.out.println("results: ");
//					 for(String key: results.keySet()) {
//						 System.out.println("	" + key + ": " + results.get(key).size() + " " + results.get(key));
//					 }
					 						 					 															 						 
//					
					if(p.hasValue() && o.hasValue()) {						
						o_index = Indexer.dictionary.get(o.getValue());
						Set<Integer> result = Indexer.pos.get(p_index).get(o_index);
						if(result != null) {
							if(results.containsKey(s.getName())) {
								//jointure de result sur results.get(s.getName())
								results = simpleJoin(results, s.getName(), result);
							}else {
								results.put(s.getName(), new ArrayList<Integer>(result));
							}	
								
						}else {
							results.put(s.getName(), new ArrayList<Integer>());
						}					
					}else if(s.hasValue() && p.hasValue()) {
						s_index = Indexer.dictionary.get(s.getValue());
						Set<Integer> result = Indexer.pso.get(p_index).get(s_index);
						if(result != null) {
							if(results.containsKey(o.getName())) {
								//jointure de result sur results.get(o.getName())
								results = simpleJoin(results, o.getName(), result);
							}else {
								results.put(o.getName(), new ArrayList<Integer>(result));
							}	
								
						}else {
							results.put(s.getName(), new ArrayList<Integer>());
						}
					}else {
						
						List<Integer> s_temp = null;
						List<Integer> o_temp = null;
						
						List<Integer> s_results = new ArrayList<Integer>();
						List<Integer> o_results = new ArrayList<Integer>();
						
						HashMap<Integer, Set<Integer>> p_temp;
																												
						if (results.containsKey(s.getName()) && !results.containsKey(o.getName())) {
							p_temp = Indexer.pso.get(p_index);
							s_temp = results.get(s.getName());
							
							for(int s_val: s_temp) {
								if(p_temp.containsKey(s_val)) {
									Set<Integer> pso_results = p_temp.get(s_val);
									for(int o_val: pso_results) {
										boolean contains = false;
										for(int i = 0; i < s_results.size(); i++) {
											if(s_results.get(i) == s_val && o_results.get(i) == o_val) {
												contains = true;
												break;
											}	
										}
										if(!contains) {
											s_results.add(s_val);
											o_results.add(o_val);
										}
									}
								}
							}
							results = join(results, s.getName(), s_results, o.getName(), o_results);
						}else if (results.containsKey(o.getName()) && !results.containsKey(s.getName())) {
							
							p_temp = Indexer.pos.get(p_index);								
							o_temp = results.get(o.getName());	
							
							for(int o_val: o_temp) {
								if(p_temp.containsKey(o_val)) {
									Set<Integer> pos_results = p_temp.get(o_val);
									for(int s_val: pos_results) {
										boolean contains = false;
										for(int i = 0; i < s_results.size(); i++) {
											if(s_results.get(i) == s_val && o_results.get(i) == o_val) {
												contains = true;
												break;
											}	
										}
										if(!contains) {
											s_results.add(s_val);
											o_results.add(o_val);
										}						
									}
								}
							}
							results = join(results, o.getName(), o_results, s.getName(), s_results);
						}else if(results.containsKey(o.getName()) && results.containsKey(s.getName())) {								
														
							o_temp = results.get(o.getName());		
							s_temp = results.get(s.getName());
							p_temp = Indexer.pso.get(p_index);								
							
							// cas d'une requete type x0 p x0
							if(s.getName().equals(o.getName())){
								for(int s_val: s_temp) {
									if(p_temp.containsKey(s_val)) {
										Set<Integer> pso_results = p_temp.get(s_val);
										for(int o_val: pso_results) {
											if(s_val == o_val && !s_results.contains(s_val)) {
												s_results.add(s_val);
												break;
											}
										}
									}
								}
								results = simpleJoin(results, s.getName(), new HashSet<Integer>(s_results));
							}else {
								
								for(int s_val: s_temp) {
									if(p_temp.containsKey(s_val)) {
										Set<Integer> pso_results = p_temp.get(s_val);
										for(int o_val: pso_results) {	
											boolean contains = false;
											for(int i = 0; i < s_results.size(); i++) {
												if(s_results.get(i) == s_val && o_results.get(i) == o_val) {
													contains = true;
													break;
												}	
											}
											if(!contains) {
												s_results.add(s_val);
												o_results.add(o_val);
											}										
										}
									}
								}								
								results = join(results, s.getName(), s_results, o.getName(), o_results);
							}																	
						}else {
							
							p_temp = Indexer.pso.get(p_index);	
							
							s_temp = new ArrayList<Integer>(p_temp.keySet());
							
							for(int s_val: s_temp) {
								if(p_temp.containsKey(s_val)) {
									Set<Integer> pso_results = p_temp.get(s_val);
									for(int o_val: pso_results) {	
										boolean contains = false;
										for(int i = 0; i < s_results.size(); i++) {
											if(s_results.get(i) == s_val && o_results.get(i) == o_val) {
												contains = true;
												break;
											}	
										}
										if(!contains) {
											s_results.add(s_val);
											o_results.add(o_val);
										}										
									}
								}
							}
							
							results.put(s.getName(), s_results);
							results.put(o.getName(), o_results);
						}															
					}						
			}		
							
		} catch (NullPointerException e) {
			//on retourne une table vide 
			results.clear();
			for(String var: variables) {
				results.put(var, new ArrayList<Integer>());
			}
		}
		
		HashMap<String, List<String>> string_results = new HashMap<String, List<String>>();
		for(String key: results.keySet()) {
			string_results.put(key, new ArrayList<String>());
			for(int ressource_index: results.get(key)) {
				string_results.get(key).add(Indexer.reverseDictionary.get(ressource_index).stringValue());
			}		
		}

		return string_results;
		
	}
	
	private List<StatementPattern> optimizeQuery(List<StatementPattern> patterns) {
		ArrayList<StatementPattern> optimizedPatterns = new ArrayList<StatementPattern>();
		int[][] selectivity = new int[patterns.size()][];
		int index = 0;
		for(StatementPattern sp: patterns) {	
			Var s = sp.getSubjectVar();
			Var p = sp.getPredicateVar();
			Var o = sp.getObjectVar();		
			if(s.hasValue() && p.hasValue()) {
				if(Indexer.dictionary.containsKey(p.getValue()) && Indexer.dictionary.containsKey(s.getValue())) {
					selectivity[index] = new int[]{index, Indexer.sp.get(Indexer.dictionary.get(s.getValue())).get(Indexer.dictionary.get(p.getValue()))};
				}else {
					return null;
				}			
			}else if(p.hasValue() && o.hasValue()) {
				if(Indexer.dictionary.containsKey(p.getValue()) && Indexer.dictionary.containsKey(o.getValue())) {
					selectivity[index] = new int[]{index, Indexer.po.get(Indexer.dictionary.get(p.getValue())).get(Indexer.dictionary.get(o.getValue()))};
				}else {
					return null;
				}	
			}else {
				if((Indexer.dictionary.containsKey(p.getValue()))) {
					selectivity[index] = new int[]{index, Indexer.p_unary_index.get(Indexer.dictionary.get(p.getValue()))};
				}else {
					return null;
				}
			}
			index++;			
		}
		
		Arrays.sort(selectivity, new Comparator<int[]>() {
			public int compare(int[] a, int[] b) {
				return a[1] - b[1];
			}
		});
		
		for(int i = 0; i < selectivity.length; i++) {
			optimizedPatterns.add(patterns.get(selectivity[i][0]));
		}
				
		return optimizedPatterns;
	}

}
