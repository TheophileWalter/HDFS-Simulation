package hdfs;

import java.util.ArrayList;
import hdfs.Node;

/*
 * Simulation d'un cluster HDFS
 * Mémoire Master 2 - Théophile Walter
 * 
 * Classe Cluster - Représente le namenode du cluster, permet l'interraction avec les noeuds
 */
public class Cluster {
	
	private int nodesNumber, filesPerNode;
	private ArrayList<Node> nodes;

	/**
	 * Constructeur du cluster, initialise les noeuds et crée les fichiers
	 * @param nodesNumber   Le nombre de noeuds à créer
	 * @param filesPerNode  Le nombre de fichiers par noeud
	 * @param blocksPerFile Le nombre de bloc pour chaque fichier
	 * @param replication   Le facteur de réplication pour chaque bloc
	 */
	public Cluster(int nodesNumber, int filesPerNode, int blocksPerFile, int replication) {
		
		// Enregistre la configuration
		this.nodesNumber = nodesNumber;
		this.filesPerNode = filesPerNode;
		
		// Crée les noeuds
		this.nodes = new ArrayList<Node>();
		for (int i = 0; i < nodesNumber; i++) {
			this.nodes.add(new Node(filesPerNode, blocksPerFile, replication));
		}
	}
	
	/**
	 * Effectue la répartition des blocs sur les noeuds
	 */
	public void shuffleBlocks() {
		// On attribue à chaque bloc les noeuds où se répliquer
		for (Node node: this.nodes) {
			node.shuffleBlocks(this);
		}
	}

	/**
	 * Retourne un noeud aléatoire du cluster
	 * @return Un noeud au hasard
	 */
	public Node getRandomNode() {
		int random = (int) Math.floor(Math.random()*this.nodes.size());
		return this.nodes.get(random);
	}

	/**
	 * Éteind un pourcentage de noeuds
	 * @param p La proportion de noeuds à éteindre [0, 1]
	 */
	public void killNodes(double p) {
		int limit = (int) (p*this.nodes.size());
		for (int i = 0; i < this.nodes.size(); i++) {
			if (i < limit) {
				this.nodes.get(i).off();
			} else {
				this.nodes.get(i).on();
			}
		}
	}

	/**
	 * Calcul la proportion des fichiers innaccessibles
	 * @return Un double entre 0 et 1
	 */
	public double getFullErrorRate() {
		// Récupère le nombre de fichiers en erreur pour chaque bloc
		int totalError = 0;
		for (Node node: this.nodes) {
			totalError += node.totalError();
		}
		return (double)totalError/(double)(this.nodesNumber*this.filesPerNode);
	}
	
}
