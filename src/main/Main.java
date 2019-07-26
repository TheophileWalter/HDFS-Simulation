package main;

import hdfs.Cluster;

/*
 * Simulation d'un cluster HDFS
 * Mémoire Master 2 - Théophile Walter
 * 
 * Classe Main - Crée un cluster et lance des simulations
 */
public class Main {

	public static void main(String... args) {
		
		// Configuration
		int nodesNumber = 1000;
		int filesPerNode = 100;
		int blocksPerFile = 10;
		int maxReplication = 10;
		
		// On crée 10 clusters avec 10 facteurs de réplication différents
		System.out.println("Création des clusters et répartition des blocs");
		Cluster[] clusters = new Cluster[maxReplication];
		for (int replication = 1; replication <= maxReplication; replication++) {
			clusters[replication-1] = new Cluster(nodesNumber, filesPerNode, blocksPerFile, replication);
			clusters[replication-1].shuffleBlocks();
		}
		
		// On fait varier la proportion de noeuds éteinds
		for (int kill = 0; kill <= 100; kill += 5) {
			
			// Pour la sortie CSV
			String line = (double)kill/100 + ";";
			
			// On fait varier la réplication
			for (int replication = 0; replication < maxReplication; replication++) {
		
				// On éteind la moitié des noeuds
				clusters[replication].killNodes((double)kill/100);
				
				// Récupère la proportion de fichiers innaccessibles
				double errorRate = clusters[replication].getFullErrorRate();
				
				// Ajoute la valeur
				line += String.format("%.5f", errorRate) + ";";
				
			}
			
			// Affiche
			System.out.println(line);
		
		}
			
	}
	
}
