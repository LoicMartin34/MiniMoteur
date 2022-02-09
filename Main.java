import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.openrdf.query.parser.sparql.SPARQLParser;

public class Main {
	
	static boolean enableVerbose = false;
	static boolean exportQueryStats = false;
	static boolean compareWithJena = false;
	static Stats stats = new Stats();
	public static boolean optimizeQuery = true;

	public static void main(String[] args) throws IOException {
	  
	  stats.startTimeMeasure("general");
		
      Options options = new Options();

      Option q = new Option("queries", true, "chemin vers le dossier des requêtes");
      q.setRequired(true);
      options.addOption(q);
      
      Option d = new Option("data", true, "chemin vers le fichier des données");
      d.setRequired(true);
      options.addOption(d);

      Option output = new Option("output", true, "chemin vers le dossier de sortie");
      options.addOption(output);
//      
      Option verbose = new Option("verbose", false, "afficher les temps d'évaluation des requêtes dans le terminal");
      options.addOption(verbose);
//      
      Option export_query_stats = new Option("export_query_stats", false, "exporter les statistiques sur les requêtes dans le dossier de sortie");
      options.addOption(export_query_stats);
//      
      Option export_query_results = new Option("export_query_results", false, "exporter les résultats des requêtes");
      options.addOption(export_query_results);
      
      Option jena = new Option("jena", false, "comparer avec Jena (non implémenté)");
      options.addOption(jena);
      
//      Option shuffle = new Option("shuffle", false, "créer une permutation sur l'ensemble des requêtes (non implémenté)");
//      options.addOption(shuffle);
//      
//      Option warm = new Option("warm", true, "évalue aléatoirement le pourcentage <arg> des requêtes en entrée avant d’évaluer le workload entier (non implémenté)");
//      options.addOption(warm);
//      
//      Option optim_none = new Option("optim_none", false, "désactiver les optimisation des requêtes (non implémenté)");
//      options.addOption(optim_none);
//      
      Option star_queries = new Option("star_queries", false, "force l'utilisation de la méthode M1");
      options.addOption(star_queries);

      CommandLineParser parser = new DefaultParser();
      HelpFormatter formatter = new HelpFormatter();

      try {
      	CommandLine cmd = parser.parse( options, args);
          String queriesDirectoryPath = cmd.getOptionValue("queries");
          String dataFilePath = cmd.getOptionValue("data"); 
          
          System.out.println("fichier des données : " + dataFilePath);
          System.out.println("dossier des requêtes : " + queriesDirectoryPath);
          
          exportQueryStats = cmd.hasOption("export_query_stats");  

          //initialisation du dictionnaire
          Indexer.init(dataFilePath);
   
          enableVerbose = cmd.hasOption("verbose");
          if(enableVerbose) {
        	  System.out.println("temps de création du dictionnaire: " + stats.getTimeMeasure("creation dictionnaire") + "ms");
        	  System.out.println("temps de création de l'index: " + stats.getTimeMeasure("creation index") + "ms");
          }
      
          boolean saveResults = false;
          FileWriter  results_file = null;
                 
          String outputPath = null;
          boolean hasOutput = cmd.hasOption("output");
          if(hasOutput) {
        	  outputPath = cmd.getOptionValue("output");
        	  System.out.println("dossier de sortie : " + outputPath);
        	  if(cmd.hasOption("export_query_results")) {
        		  saveResults = true;
        		  results_file = new FileWriter(outputPath + "/results.txt");
        	  }
          }
          
          compareWithJena = cmd.hasOption("jena");
          List<Boolean> jenaCompareResult = null;
          if(compareWithJena) {
        	  jenaCompareResult = new ArrayList<Boolean>();
        	  System.out.println("initialisation de Jena");
        	  JenaSPARQL.init(dataFilePath); 	  
          }
          
          boolean useStarQueries = false;
          
          Engine engine;
          
          if(cmd.hasOption("star_queries")){
        	  System.out.println("utilisation de la méthode M1");
        	  engine = new M1();
        	  useStarQueries = true;
          }else {
        	  engine = new M2();
        	  System.out.println("utilisation de la méthode M2");
          }
          
          //chargement des requêtes
          stats.startTimeMeasure("temps lecture requetes");
          File directory = new File(queriesDirectoryPath);
          File[] contents = directory.listFiles();                                
          List<String> queries = new ArrayList<String>();
          for (File f: contents) {
        	  queries.addAll(App.parseFile(f.getAbsolutePath()));
          }
          stats.pauseTimeMeasure("temps lecture requetes");
          System.out.println("chargement de " + queries.size() + " requetes");
          if(Main.exportQueryStats) {
	  	      Main.stats.saveMeasure("queries number", Integer.toString(queries.size()));
	  	  }
          
          //evaluation des requetes
          List<HashMap<String, List<String>>> all_results = new ArrayList<HashMap<String, List<String>>>();
          
          Main.stats.startTimeMeasure("evaluation");
          for(String query: queries) {
        	  all_results.add(engine.processQuery(query));
        	  
          }
          Main.stats.pauseTimeMeasure("evaluation");
          
          boolean completudeTest = false;
          if(compareWithJena) {
        	  System.out.println("evaluation avec Jena ...");
			  ArrayList<HashMap<String, List<String>>> jenaResults = JenaSPARQL.run(queries);		  
			  completudeTest = compare(jenaResults, all_results);
		  }
                  
          System.out.println("toutes les requêtes ont été traitées avec succès");
          
          if(enableVerbose) {
    			System.out.println("temps total de l'évaluation des requêtes: " + stats.getTimeMeasure("evaluation") + "ms");
    	  }
          
          if(compareWithJena) {
			  System.out.println("résultat de la comparaison avec Jena: " + completudeTest);
		  } 
          
          if(saveResults) {
    		  for(HashMap<String, List<String>> res: all_results) {
    			  results_file.write(printTable(res) + "\n");   
    		  }
    		  System.out.println("le résultat des requêtes est sauvegardé dans : " + outputPath + "/results.txt");
        	  results_file.close();    		  
    	  } 

          stats.pauseTimeMeasure("general");
          if(exportQueryStats) {
        	  String headTuple = new String("nom du fichier de données,nom du dossier des requêtes,nombre de triplets RDF,nombre de requêtes,temps de lecture des requêtes (ms),temps création dico (ms),nombre d’index,temps total création des index (ms),temps d’évaluation du workload (ms),temps pris par l’optimisation (ms),temps total (du début à la fin du programme) (ms) \n");
        	  String[] statsTuple = {dataFilePath, queriesDirectoryPath, stats.getMeasure("triples number"), stats.getMeasure("queries number"), Float.toString(stats.getTimeMeasure("temps lecture requetes")), Long.toString(Main.stats.getTimeMeasure("creation dictionnaire")), Integer.toString(Indexer.nb_index), Float.toString(stats.getTimeMeasure("creation index")),  Long.toString(Main.stats.getTimeMeasure("evaluation")), "NON_DISPONIBLE", Float.toString(stats.getTimeMeasure("general"))};
        	  FileWriter csvWriter = new FileWriter(outputPath + "/new.csv");
        	  csvWriter.append(headTuple);
        	  for(int i = 0; i < statsTuple.length; i++) {
        		  csvWriter.append(statsTuple[i]);
        		  if(i < statsTuple.length - 1) { csvWriter.append(","); }
        		  else { csvWriter.append("\n"); }
        	  }
        	  csvWriter.flush();
        	  csvWriter.close();
        	  
          }
          System.out.println("durée totale du programme: " + stats.getTimeMeasure("general") + "ms");
          
          //PREPARATION CSV
          
          //nom du fichier de données | nom du dossier des requêtes | nombre de triplets
//          RDF | nombre de requêtes | temps de lecture des requêtes (ms) | temps
//          création dico (ms) | nombre d’index | temps total création des index (ms) |
//          temps d’évaluation du workload (ms) | temps pris par l’optimisation (ms) |
//          temps total (du début à la fin du programme) (ms)

      } catch (ParseException e) {
          System.out.println(e.getMessage());
          formatter.printHelp("utility-name", options);

          System.exit(1);
      } catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}
	
	

	private static boolean compare(List<HashMap<String, List<String>>> jenaResults,
			List<HashMap<String, List<String>>> file_results) {
		
		for(int i = 0;i < jenaResults.size(); i++) {
			String baseKey = jenaResults.get(i).keySet().iterator().next();
			for(int k = 0; k < file_results.get(i).get(baseKey).size(); k++) {
				boolean found = true;		
					for(String key: jenaResults.get(i).keySet()) {
						if(!jenaResults.get(i).get(key).get(0).equals(file_results.get(i).get(key).get(k))) {
							found = false;
							break;
						}
				}			
				if(found) {	
					for(String key: jenaResults.get(i).keySet()) {
						jenaResults.get(i).get(key).remove(0);
						file_results.get(i).get(key).remove(k);	
					}
					k = -1;
				}
			}
		}

		for(HashMap<String, List<String>> res: file_results) {
			for(List<String> list: res.values()) {
				if(list.size() > 0) { return false; }
			}
		}
		
		return true;
		
	}
	
public static String printTable(HashMap<String, List<String>> results) {
		
		List<List<String>> rows = new ArrayList<>();
		List<String> headers = new ArrayList<String>(results.keySet());
		rows.add(headers);

		for(int i = 0; i < results.get(headers.get(0)).size(); i++) {
			List<String> row = new ArrayList<String>();
			for(List<String> result: results.values()) {
				row.add(result.get(i));
			}
			rows.add(row);
		}
				
	    int[] maxLengths = new int[rows.get(0).size()];
	    for (List<String> row : rows)
	    {
	        for (int i = 0; i < row.size(); i++)
	        {
	            maxLengths[i] = Math.max(maxLengths[i], row.get(i).length());
	        }
	    }

	    StringBuilder formatBuilder = new StringBuilder();
	    for (int maxLength : maxLengths)
	    {
	        formatBuilder.append("%-").append(maxLength + 2).append("s");
	    }
	    String format = formatBuilder.toString();

	    StringBuilder result = new StringBuilder();
	    for (List<String> row : rows)
	    {
	        result.append(String.format(format, row.toArray(new String[0]))).append("\n");
	    }
	    
	    return result.toString();
	}
}
