package test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import utils.AprioriUtils;
import utils.HashTreeUtils;

import model.HashTreeNode;
import model.ItemSet;
import model.Transaction;

public class TestMRJob {

	public static void main(String[] args) {
		List<ItemSet> largeItemsetsPrevPass = getLargeItemSets();
		List<ItemSet> candidateItemsets = AprioriUtils.getCandidateItemsets(largeItemsetsPrevPass, 1);
		System.out.println("Candidate itemsets " + Arrays.toString(candidateItemsets.toArray()));
		HashTreeNode hashTreeRootNode = HashTreeUtils.buildHashTree(candidateItemsets, 2); // This would be changed later
		
		List<Transaction> txns = getTransactions();
		for(Transaction txn : txns) {
			List<ItemSet> candidateItemsetsInTxn = HashTreeUtils.findItemsets(hashTreeRootNode, txn, 0);
			System.out.println("Itemsets for txn " + txn.toString() + " are " + Arrays.toString(candidateItemsetsInTxn.toArray()));
		}

		
	}

	private static List<ItemSet> getLargeItemSets()
	{
		List<ItemSet> itemsets = new ArrayList<ItemSet>();
		List<Integer> items1 = new ArrayList<Integer>();items1.add(1);
		itemsets.add(new ItemSet(items1, 2));
		
		List<Integer> items2 = new ArrayList<Integer>();items2.add(2);
		itemsets.add(new ItemSet(items2, 3));
		
		List<Integer> items3 = new ArrayList<Integer>();items3.add(3);
		itemsets.add(new ItemSet(items3, 3));
		
		List<Integer> items4 = new ArrayList<Integer>();items4.add(5);
		itemsets.add(new ItemSet(items4, 3));
		
		System.out.println("Large itemsets : " + Arrays.toString(itemsets.toArray()));
		return itemsets;
	}
	
	private static List<Transaction> getTransactions()
	{
		List<Transaction> txns = new ArrayList<Transaction>();
		
		txns.add(AprioriUtils.getTransaction("1 1 3 4"));
		txns.add(AprioriUtils.getTransaction("2 2 3 5"));
		txns.add(AprioriUtils.getTransaction("3 1 2 3 5"));
		txns.add(AprioriUtils.getTransaction("4 2 5"));
		
		return txns;
	}
}
