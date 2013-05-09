package apriori;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import model.HashTreeNode;
import model.ItemSet;
import model.Transaction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import utils.AprioriUtils;
import utils.HashTreeUtils;

/*
 * A parallel hadoop-based Apriori algorithm
 */
public class MRApriori {
	private static String jobPrefix = "MRApriori Algorithm Phase ";
	private static String hdfsInputDir = "/user/hduser/mrapriori-T10I4D100K";
	private static String hdfsOuptutDirPrefix = "/user/hduser/mrapriori-out-";

	// TODO : Can I dynamically determine this?
	private static int MAX_PASSES = 4; // T10_I4_D100K 
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration phase1Conf = new Configuration();

		// Set phase-1 MR job
		System.out.println("Starting AprioriPhase1Job");
		int passNum = 1;
		Job aprioriPhase1Job = new Job(phase1Conf, jobPrefix + passNum);
		configureAprioriJob(aprioriPhase1Job, AprioriPhase1Mapper.class);
		FileInputFormat.addInputPath(aprioriPhase1Job, new Path(hdfsInputDir));
		FileOutputFormat.setOutputPath(aprioriPhase1Job, new Path(hdfsOuptutDirPrefix + passNum));
		boolean isDone = aprioriPhase1Job.waitForCompletion(true) ? true : false;
		System.out.println("Finished AprioriPhase1Job");
		
		// Once phase-1 is done, process other phases
		Configuration phaseKConf = null;
		Job aprioriPhaseKJob = null;
		for(int k=2; k <= MAX_PASSES; k++) {
			++passNum;
			System.out.println("Starting AprioriPhase" + passNum +"Job");
			phaseKConf = new Configuration();
			phaseKConf.setInt("passNum", passNum);
			
			// Add output of previous run to the distributed cache
			
			// TODO : change this to add all files from output directory 
			DistributedCache.addCacheFile(URI.create("hdfs://localhost:54310/user/hduser/mrapriori-out-" + (passNum-1) + "/part-r-00000"), phaseKConf);
			System.out.println("Added to distributed cache the output of pass " + (passNum-1));
			
			aprioriPhaseKJob = new Job(phaseKConf, jobPrefix + passNum);
			configureAprioriJob(aprioriPhaseKJob, AprioriPhaseKMapper.class);
			FileInputFormat.addInputPath(aprioriPhaseKJob, new Path(hdfsInputDir));
			FileOutputFormat.setOutputPath(aprioriPhaseKJob, new Path(hdfsOuptutDirPrefix + passNum));
			isDone = aprioriPhaseKJob.waitForCompletion(true) ? true : false;
			
			System.out.println("Finishing AprioriPhase" + passNum +"Job");
		}
	}

	/*
	 * Configures a map-reduce job for Apriori based on the parameters like pass number, mapper etc.
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static void configureAprioriJob(Job aprioriJob, Class mapperClass)
	{
		aprioriJob.setJarByClass(MRApriori.class);
		aprioriJob.setMapperClass(mapperClass);
		aprioriJob.setReducerClass(AprioriReducer.class);
		aprioriJob.setOutputKeyClass(Text.class);
		aprioriJob.setOutputValueClass(IntWritable.class);
	}

	//------------------ Utility functions ----------------------------------
	// Phase1 - MapReduce
	/*
	 * Mapper for Phase1 would emit a <itemId, 1> pair for each item across all transactions.
	 */
	public static class AprioriPhase1Mapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text item = new Text();

		public void map(Object key, Text txnRecord, Context context) throws IOException, InterruptedException {
			Transaction txn = AprioriUtils.getTransaction(txnRecord.toString());
			for(Integer itemId : txn.getItems()) {
				item.set(itemId.toString());
				context.write(item, one);
			}
		}
	}

	/*
	 * Reducer for all phases would collect the emitted itemId keys from all the mappers
	 * and aggregate it to return the count for each itemId.
	 */
	public static class AprioriReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text itemset, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int countItemId = 0;
			for (IntWritable value : values) {
				countItemId += value.get();
			}

			// TODO : This can be improved. Creating too many strings.
			String itemsetIds = itemset.toString();
			itemsetIds = itemsetIds.replace("[", "");
			itemsetIds = itemsetIds.replace("]", "");
			itemsetIds = itemsetIds.replace(" ", "");

			// If the item has minSupport, then it is a large itemset.
			if(AprioriUtils.hasMinSupport(countItemId)) {
				context.write(new Text(itemsetIds), new IntWritable(countItemId));	
			}
		}
	}
	
	
	// Phase2 - MapReduce
	/*
	 * Mapper for PhaseK would emit a <itemId, 1> pair for each item across all transactions.
	 */
	public static class AprioriPhaseKMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text item = new Text();

		private List<ItemSet> largeItemsetsPrevPass = new ArrayList<ItemSet>();
		private List<ItemSet> candidateItemsets     = null;
		private HashTreeNode hashTreeRootNode       = null;
		
		@Override
		public void setup(Context context) throws IOException {
			Path[] uris = DistributedCache.getLocalCacheFiles(context.getConfiguration());

			int passNum = context.getConfiguration().getInt("passNum", 2);
			String opFileLastPass = "mrapriori-out-" + (passNum-1);
			System.out.println("Distributed cache file to search " + opFileLastPass);
			BufferedReader fis;
			for (int i = 0; i < uris.length; i++) {
				if (uris[i].toString().contains(opFileLastPass)) {
					String currLine = null;
					fis = new BufferedReader(new FileReader(uris[i].toString()));
					while ((currLine = fis.readLine()) != null) {
						currLine = currLine.trim();
						String[] words = currLine.split("[\\s\\t]+");
						if(words.length < 2) {
							continue;
						}
						
						List<Integer> items = new ArrayList<Integer>();
						for(int k=0; k < words.length -1 ; k++){
							String csvItemIds = words[k];
							String[] itemIds = csvItemIds.split(",");
							for(String itemId : itemIds) {
								items.add(Integer.parseInt(itemId));
							}
						}
						String finalWord = words[words.length-1];
						int supportCount = Integer.parseInt(finalWord);
						
						largeItemsetsPrevPass.add(new ItemSet(items, supportCount));
					}
				}
			}
			
			candidateItemsets = AprioriUtils.getCandidateItemsets(largeItemsetsPrevPass, (passNum-1));
			hashTreeRootNode = HashTreeUtils.buildHashTree(candidateItemsets, passNum); // This would be changed later
		}

		public void map(Object key, Text txnRecord, Context context) throws IOException, InterruptedException {
			Transaction txn = AprioriUtils.getTransaction(txnRecord.toString());
			List<ItemSet> candidateItemsetsInTxn = HashTreeUtils.findItemsets(hashTreeRootNode, txn, 0);
			for(ItemSet itemset : candidateItemsetsInTxn) {
				item.set(itemset.getItems().toString());
				context.write(item, one);
			}
			
		}
	}

}
