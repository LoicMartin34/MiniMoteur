import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import fr.lirmm.graphik.graal.api.core.Atom;
import fr.lirmm.graphik.graal.api.core.AtomSet;
import fr.lirmm.graphik.graal.api.core.ConjunctiveQuery;
import fr.lirmm.graphik.graal.api.core.Term;
import fr.lirmm.graphik.graal.io.sparql.SparqlConjunctiveQueryParser;
import fr.lirmm.graphik.util.stream.CloseableIterator;

public class App {
	public static void main(String[] args) throws Exception {
		
		// Parser le fichier de requêtes
		ArrayList<String> all_queries_from_file = parseFile("C://Users/pierr/Desktop/ProjetMoteurRDF/Requetes/100/Q_1_eligibleregion_100.queryset");
		
		// Parcours des requêtes individuelles
		for(String query : all_queries_from_file) {
			
			// Parsage de la requête en utilisant le parseur de Graal
			SparqlConjunctiveQueryParser queryParser = new SparqlConjunctiveQueryParser(query);
	
			// Description du résultat
			ConjunctiveQuery parsed_query = queryParser.getConjunctiveQuery();
			
			// Les variables réponses de la requête
			List<Term> answer_variables = parsed_query.getAnswerVariables();
	
			// Les conditions de la clause WHERE
			AtomSet conditions = parsed_query.getAtomSet();
			CloseableIterator<Atom> it = conditions.iterator();
	
			while(it.hasNext()) {
				Atom condition = (Atom)it.next();
				// Sujet
				String sujet = condition.getTerm(0).toString();
				
				// Prédicat
				String predicat = condition.getPredicate().getIdentifier().toString();
				
				// Objet
				String objet = condition.getTerm(1).toString();
				
			}
		}

	}
	
	/**
	 * Parse un fichier de requêtes SPARQL dont le délimiteur est une accolade fermante
	 * et retourne une liste contenant l'ensemble des requêtes sous forme de String
	 * @param fileName le chemin vers le fichier de requêtes à parser
	 * @return la liste des requêtes
	 */
	static ArrayList<String> parseFile(String fileName) {

		ArrayList<String> res = new ArrayList<String>();
		try {
			Scanner reader = new Scanner(new File(fileName));
			reader.useDelimiter("}");
			while (reader.hasNext()) {
				String data = reader.next().trim();
				if(!data.isEmpty()) {
					res.add(data + "}");
				}
			}
			reader.close();
		} catch (FileNotFoundException e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		return res;
	}
}
