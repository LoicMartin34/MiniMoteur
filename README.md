# MiniMoteur
Projet de M2 consistant à réaliser un moteur de requêtes SPARQL ainsi que la réalisation d'un benchmark.


Utilisation du programme :

java -jar rdfengine -DossierRequetes -dataset -fichierResultats.queryset

Il faut bien respecter l'ordre de ces paramètres, les utiliser obligatoirement, et doivent se situer juste après le nom du fichier jar.
   1 : dossier de requêtes
   2 : fichier dataset
   3 : fichier résultats des requêtes

Ensuite ces paramètres sont disponibles (le "-" est requis dans l'utilisation des arguments) :
	-verbose : pour avoir l'affichage terminal
	-output : obtenir le fichier "output" qui contient les temps d'exécutions
	-jena : pour obtenir un fichier "compareJena" qui donne les résultats du programme et de Jena + le true ou false
