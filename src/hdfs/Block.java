package hdfs;

import java.util.ArrayList;

/*
 * Simulation d'un cluster HDFS
 * Mémoire Master 2 - Théophile Walter
 * 
 * Classe Block - Représente un bloc d'un fichier
 */
public class Block {
	
	private int replication;
	private ArrayList<Node> nodes;

	/**
	 * Crée un bloc d'un fichier avec un certain facteur de réplication
	 * Chaque bloc doit être associé à des Node, ce sont les noeuds sur lesquels il est hébergé.
	 * Cela permet de vérifier si le bloc est disponnible
	 * @param replication Facteur de réplication de ce bloc
	 */
	public Block(int replication) {
		this.replication = replication;
		
		// Crée la liste dans laquelle seront enregistrés les noeuds hébergeant le bloc actuel
		this.nodes = new ArrayList<Node>();
	}
	
	/**
	 * Associe un noeud au bloc, il s'agit du noeud où est répliqué le bloc
	 * @param node Un noeud
	 * @return -1 si le bloc est sous-répliqué
	 *          0 si le bloc est correctement répliqué
	 *         +1 si le bloc est sur-répliqué
	 */
	public int addNode(Node node) {
		this.nodes.add(node);
		if (this.replication > this.nodes.size()) {
			return -1;
		} else if (this.replication < this.nodes.size()) {
			return 1;
		} else {
			return 0;
		}
	}
	
	/**
	 * Vérifie si le bloc est accessible, si au moins un des noeuds
	 * qui l'hébèrge est disponnible
	 * @return true si le bloc est accessible
	 *         false si il est innaccessible
	 */
	public boolean accessBlock() {
		for (Node node: this.nodes) {
			if (node.isUp()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Effectue la répartition des blocs sur les noeuds
	 * @param cluster Le Cluster actuel (pour obtenir un noeud)
	 */
	public void shuffleBlocks(Cluster cluster) {
		// On réplique le bloc le bon nombre de fois
		int size = this.nodes.size();
		for (int i = 0; i < this.replication-size; i++) {
			this.addNode(cluster.getRandomNode());
		}
	}

}
