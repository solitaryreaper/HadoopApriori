package model;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Represents itemsets i.e. a group of items that are bought together in a transaction, along
 * with their support counts in the input transaction dataset.
 * 
 * @author shishir
 *
 */
public class ItemSet implements Comparable<ItemSet> 
{
	private List<Integer> items;
	private int supportCount;
	
	public ItemSet(List<Integer> items, int supportCount) {
		super();
		this.items = items;
		this.supportCount = supportCount;
		
		Collections.sort(this.items);
	}

	@Override
	public String toString() {
		return "ItemSet [items=" + Arrays.toString(items.toArray()) + ", supportCount=" + supportCount + "]";
	}

	
	// Two itemsets are equal if they have the same set of items.

	public List<Integer> getItems() {
		return items;
	}

	public void setItems(List<Integer> items) {
		this.items = items;
	}

	public int getSupportCount() {
		return supportCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((items == null) ? 0 : items.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ItemSet other = (ItemSet) obj;
		if (items == null) {
			if (other.items != null)
				return false;
		} else if (!items.equals(other.items))
			return false;
		return true;
	}

	public void setSupportCount(int supportCount) {
		this.supportCount = supportCount;
	}

	@Override
	public int compareTo(ItemSet that) {
		List<Integer> thisItems = this.getItems();
		List<Integer> thatItems = that.getItems();
		if(thisItems == thatItems) {
			return 0;
		}
		
		for(int i=0; i < thisItems.size(); i++) {
			int diff = thisItems.get(i).compareTo(thatItems.get(i));
			if(diff != 0) {
				return diff;
			}
		}

		return 0;
	}

	
}
