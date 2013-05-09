package utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import model.HashTreeNode;
import model.ItemSet;
import model.Transaction;

/**
 * Utility class that builds a hash tree for fast searching for itemset patterns in a transaction.
 */
public class HashTreeUtils 
{
	/*
	 * Builds hashtree from the candidate itemsets.
	 */
	public static HashTreeNode buildHashTree(List<ItemSet> candidateItemsets, int itemsetSize)
	{
		HashTreeNode hashTreeRoot = new HashTreeNode();
		
		HashTreeNode parentNode = null;
		HashTreeNode currNode = null;
		for(ItemSet currItemset : candidateItemsets) {
			parentNode = null;
			currNode = hashTreeRoot;
			for(int i=0; i < itemsetSize; i++) {
				Integer item = currItemset.getItems().get(i);
				Map<Integer, HashTreeNode> mapAtNode = currNode.getMapAtNode();
				parentNode = currNode;
				
				if(mapAtNode.containsKey(item)) {
					currNode = mapAtNode.get(item);
				}
				else {
					currNode = new HashTreeNode();
					mapAtNode.put(item, currNode);
				}
				
				parentNode.setMapAtNode(mapAtNode);
			}
			
			currNode.setLeafNode(true);
			List<ItemSet> itemsets = currNode.getItemsets();
			itemsets.add(currItemset);
			currNode.setItemsets(itemsets);
		}
		
		return hashTreeRoot;
	}
	
	/*
	 * Returns the set of itemsets in a transaction from the set of candidate itemsets. Used hash tree
	 * data structure for fast generation of matching itemsets.
	 */
	public static List<ItemSet> findItemsets(HashTreeNode hashTreeRoot, Transaction t, int startIndex)
	{
		if(hashTreeRoot.isLeafNode()) {
			return hashTreeRoot.getItemsets();
		}

		List<ItemSet> matchedItemsets = new ArrayList<ItemSet>();
		for(int i=startIndex; i < t.getItems().size(); i++) {
			Integer item = t.getItems().get(i);
			Map<Integer, HashTreeNode> mapAtNode = hashTreeRoot.getMapAtNode();

			if(!mapAtNode.containsKey(item)) {
				continue;
			}
			List<ItemSet> itemset = findItemsets(mapAtNode.get(item), t, i+1);
			matchedItemsets.addAll(itemset);
		}
		
		return matchedItemsets;
	}
	
	/*
	 * Prints the hashtree for debugging purposes.
	 */
	public static void printHashTree(HashTreeNode hashTreeRoot)
	{
		if(hashTreeRoot == null) {
			System.out.println("Hash Tree Empty !!");
			return;
		}
		
		System.out.println("Node " + hashTreeRoot.toString());
		Map<Integer, HashTreeNode> mapAtNode = hashTreeRoot.getMapAtNode();
		for(Map.Entry<Integer, HashTreeNode> entry : mapAtNode.entrySet()) {
			printHashTree(entry.getValue());	
		}
		
 	}
}
