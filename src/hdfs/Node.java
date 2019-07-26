package hdfs;

import java.util.ArrayList;

/*
 * Simulation d'un cluster HDFS
 * Mémoire Master 2 - Théophile Walter
 * 
 * Classe Node - Représente un noeud (ordinateur)
 */
public class Node {
	
	private ArrayList<File> files;
	private boolean status = true;

	/**
	 * Crée un noeud et les fichiers correspondants
	 * @param filesPerNode  Nombre de fichiers dans le noeud
	 * @param blocksPerFile Nombre de blocs pour chaque fichier
	 * @param replication   Facteur de réplication de chaque bloc
	 */
	public Node(int filesPerNode, int blocksPerFile, int replication) {
		// Crée les fichiers du bloc
		this.files = new ArrayList<File>();
		for (int i = 0; i < filesPerNode; i++) {
			this.files.add(new File(blocksPerFile, replication));
		}
	}
	
	/**
	 * Marque le noeud comme accessible
	 */
	public void on() {
		this.status = true;
	}
	
	/**
	 * Marque le noeud comme innaccessible
	 */
	public void off() {
		this.status = false;
	}

	/**
	 * Vérifie si le noeud actuel est accessible
	 * @return true si le noeud est accessible
	 *         false sinon
	 */
	public boolean isUp() {
		return this.status;
	}
	
	/**
	 * Renvoie un fichier au hasard dans le noeud
	 * @return Un fichier au hasard
	 */
	public File getRandomFile() {
		int random = (int) Math.floor(Math.random()*this.files.size());
		return this.files.get(random);
	}

	/**
	 * Effectue la répartition des blocs sur les noeuds
	 * @param cluster Le Cluster actuel (pour obtenir un noeud)
	 */
	public void shuffleBlocks(Cluster cluster) {
		// On attribue à chaque bloc les noeuds où se répliquer
		for (File file: this.files) {
			file.shuffleBlocks(cluster);
		}
	}

	/**
	 * Retourne le nombre de fichiers innaccessibles pour ce bloc
	 * @return Un entier entre 0 et le nombre de fichiers présents
	 */
	public int totalError() {
		int total = 0;
		for (File file: this.files) {
			if (!file.accessFile()) {
				total++;
			}
		}
		return total;
	}

}
