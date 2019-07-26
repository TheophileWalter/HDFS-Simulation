package hdfs;

import java.util.ArrayList;

/*
 * Simulation d'un cluster HDFS
 * Mémoire Master 2 - Théophile Walter
 * 
 * Classe File - Représente un fichier, constitué de plusieurs blocs répartis sur des noeuds
 */
public class File {

	private int blocksPerFile;
	private ArrayList<Block> blocks;
	
	/**
	 * Crée un fichier et les blocs le composant
	 * @param blocksPerFile Nombre de blocs dans le fichier
	 * @param replication   Facteur de réplication de chaque bloc
	 */
	public File(int blocksPerFile, int replication) {

		// Enregistre les paramètres
		this.blocksPerFile = blocksPerFile;
		
		// Crée les blocs du fichier
		this.blocks = new ArrayList<Block>();
		for (int i = 0; i < blocksPerFile; i++) {
			this.blocks.add(new Block(replication));
		}
		
	}
	
	/**
	 * Vérifie si le fichier est accessible
	 * Il l'est si tous ses blocs le sont
	 * @return true si le fichier est accessible
	 *         false sinon
	 */
	public boolean accessFile() {
		int count = 0;
		for (Block block: this.blocks) {
			if (block.accessBlock()) {
				count++;
			}
		}
		return count == this.blocksPerFile;
	}

	/**
	 * Effectue la répartition des blocs sur les noeuds
	 * @param cluster Le Cluster actuel (pour obtenir un noeud)
	 */
	public void shuffleBlocks(Cluster cluster) {
		// On attribue à chaque bloc les noeuds où se répliquer
		for (Block block: this.blocks) {
			block.shuffleBlocks(cluster);
		}
	}

}
