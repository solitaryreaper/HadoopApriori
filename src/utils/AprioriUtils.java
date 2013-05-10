package utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import model.ItemSet;
import model.Transaction;

/*
 * Contains utility methods for Apriori algorithm
 */
public class AprioriUtils {

	/*
	 * Returns a transaction object for the input txn record.
	 */
	public static Transaction getTransaction(String txnRecord)
	{
		Transaction transaction = null;

		String currLine = txnRecord.trim();
		String[] words = currLine.split("[\\s\\t]+");

		int currTid = Integer.parseInt(words[0].trim());
		List<Integer> currItems = new ArrayList<Integer>();
		for(int i=1; i < words.length; i++) {
			currItems.add(Integer.parseInt(words[i].trim()));
		}
		
		transaction = new Transaction(currTid, currTid, currItems);
		
		return transaction;
	}

	/*
	 * Determines if an item with the specified frequency has minimum support or not.
	 */
	public static boolean hasMinSupport(double minSupPercent, int maxNumTxns, int itemCount)
	{
		boolean hasMinSupport = false;
		int minSupport = (int)((double)(minSupPercent * maxNumTxns))/100;
		if(itemCount >= minSupport) {
			hasMinSupport = true;
		}

		return hasMinSupport;
	}

	/*
	 * Generates the candidateItemSets of pass K from large itemsets of pass K-1.
	 */
	public static List<ItemSet> getCandidateItemsets(List<ItemSet> largeItemSetPrevPass, int itemSetSize)
	{
		List<ItemSet> candidateItemsets = new ArrayList<ItemSet>();
		List<Integer> newItems = null;
		Map<Integer, List<ItemSet>> largeItemsetMap = getLargeItemsetMap(largeItemSetPrevPass);
		Collections.sort(largeItemSetPrevPass);
		
		for(int i=0; i < (largeItemSetPrevPass.size() -1); i++) {
			for(int j=i+1; j < largeItemSetPrevPass.size() ; j++) {
				List<Integer> outerItems = largeItemSetPrevPass.get(i).getItems();
				List<Integer> innerItems = largeItemSetPrevPass.get(j).getItems();
				
				if((itemSetSize - 1) > 0) {
					boolean isMatch = true;
					for(int k=0; k < (itemSetSize -1); k++) {
						if(!outerItems.get(k).equals(innerItems.get(k))) {
							isMatch = false;
							break;
						}
					}
					
					
					if(isMatch) {
						newItems = new ArrayList<Integer>();
						newItems.addAll(outerItems);
						newItems.add(innerItems.get(itemSetSize-1));
						
						ItemSet newItemSet = new ItemSet(newItems, 0);
						if(prune(largeItemsetMap, newItemSet)) {
							candidateItemsets.add(newItemSet);
						}
					}
				}
				else {
					if(outerItems.get(0) < innerItems.get(0)) {
						newItems = new ArrayList<Integer>();
						newItems.add(outerItems.get(0));
						newItems.add(innerItems.get(0));

						ItemSet newItemSet = new ItemSet(newItems, 0);
						
						candidateItemsets.add(newItemSet);
					}
				}
			}
		}
		return candidateItemsets;
	}
	
	/*
	 * Generates a map of hashcode and the corresponding itemset. Since multiple entries can
	 * have the same hashcode, there would be a list of itemsets for any hashcode.
	 */
	public static Map<Integer, List<ItemSet>> getLargeItemsetMap(List<ItemSet> largeItemsets)
	{
		Map<Integer, List<ItemSet>> largeItemsetMap = new HashMap<Integer, List<ItemSet>>();
		
		List<ItemSet> itemsets = null;
		for(ItemSet largeItemset : largeItemsets) {
			int hashCode = largeItemset.hashCode();
			if(largeItemsetMap.containsKey(hashCode)) {
				itemsets = largeItemsetMap.get(hashCode);
			}
			else {
				itemsets = new ArrayList<ItemSet>();
			}

			itemsets.add(largeItemset);
			largeItemsetMap.put(hashCode, itemsets);
		}

		return largeItemsetMap;
	}

	private static boolean prune(Map<Integer, List<ItemSet>> largeItemsetsMap, ItemSet newItemset)
	{
		List<ItemSet> subsets = getSubsets(newItemset);

		for(ItemSet s : subsets) {
			boolean contains = false;
			int hashCodeToSearch = s.hashCode();
			if(largeItemsetsMap.containsKey(hashCodeToSearch)) {
				List<ItemSet> candidateItemsets = largeItemsetsMap.get(hashCodeToSearch);
				for(ItemSet itemset : candidateItemsets) {
					if(itemset.equals(s)) {
						contains = true;
						break;
					}
				}
			}

			if(!contains)
				return false;
		}

		return true;
	}
	
	/*
	 * Generate all possible k-1 subsets for this itemset (preserves order)
	 */
	private static List<ItemSet> getSubsets(ItemSet itemset)
	{
		List<ItemSet> subsets = new ArrayList<ItemSet>();

		List<Integer> items = itemset.getItems();
		for(int i = 0; i < items.size(); i++) {
			List<Integer> currItems = new ArrayList<Integer>(items);
			currItems.remove(items.size() - 1 - i);
			subsets.add(new ItemSet(currItems, 0));
		}

		return subsets;
	}
}
