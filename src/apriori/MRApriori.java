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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.AprioriUtils;
import utils.HashTreeUtils;

/*
 * A parallel hadoop-based Apriori algorithm
 */
public class MRApriori extends Configured implements Tool
{
	private static String jobPrefix = "MRApriori Algorithm Phase ";

	// TODO : This is bad as I using a global shared variable between functions which should
	// ideally be a function parameter. Need to fix this later. These parameters are required in
	// reducer logic and have to be dynamica. How can I pass some initialisation parameters to
	// reducer ?
	private static Double MIN_SUPPORT_PERCENT = 0.75;
	private static Integer MAX_NUM_TXNS = 98395;
	
	public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		if(args.length != 5) {
			System.err.println("Incorrect number of command line args. Exiting !!");
			return -1;
		}
		
		String hdfsInputDir = args[0];
		String hdfsOutputDirPrefix = args[1];
		
		int maxPasses = Integer.parseInt(args[2]);
		MIN_SUPPORT_PERCENT = Double.parseDouble(args[3]);
		MAX_NUM_TXNS = Integer.parseInt(args[4]);
		
		System.out.println("InputDir 		 : " + hdfsInputDir);
		System.out.println("OutputDir Prefix : " + hdfsOutputDirPrefix);
		System.out.println("Number of Passes : " + maxPasses);
		System.out.println("MinSupPercent : " + MIN_SUPPORT_PERCENT);
		System.out.println("Max Txns : " + MAX_NUM_TXNS);
		for(int passNum=1; passNum <= maxPasses; passNum++) {
			boolean isPassKMRJobDone = runPassKMRJob(hdfsInputDir, hdfsOutputDirPrefix, passNum);
			if(!isPassKMRJobDone) {
				System.err.println("Phase1 MapReduce job failed. Exiting !!");
				return -1;
			}
		}
		
		return 1;
	}

	/*
	 * Runs the pass K mapreduce job for apriori algorithm. 
	 */
	private static boolean runPassKMRJob(String hdfsInputDir, String hdfsOutputDirPrefix, int passNum) 
				throws IOException, InterruptedException, ClassNotFoundException
	{
		boolean isMRJobSuccess = false;
		
		Configuration passKMRConf = new Configuration();
		passKMRConf.setInt("passNum", passNum);
		System.out.println("Starting AprioriPhase" + passNum +"Job");

		// TODO : Should I scan all files in this output directory?
		/*
		 * The large itemsets of pass (K-1) are used to derive the candidate itemsets of pass K.
		 * So, storing the large itemsets of pass (K-1) in distributed cache, so that it is 
		 * accessible to pass K MR job.
		 */
		if(passNum > 1) {
			DistributedCache.addCacheFile(
					URI.create("hdfs://localhost:54310" + hdfsOutputDirPrefix + 
					(passNum-1) + "/part-r-00000"), passKMRConf
			);
			System.out.println("Added to distributed cache the output of pass " + (passNum-1));
		}

		Job aprioriPassKMRJob = new Job(passKMRConf, jobPrefix + passNum);
		if(passNum == 1) {
			configureAprioriJob(aprioriPassKMRJob, AprioriPass1Mapper.class);
		}
		else {
			configureAprioriJob(aprioriPassKMRJob, AprioriPassKMapper.class);
		}

		FileInputFormat.addInputPath(aprioriPassKMRJob, new Path(hdfsInputDir));
		FileOutputFormat.setOutputPath(aprioriPassKMRJob, new Path(hdfsOutputDirPrefix + passNum));
		
		isMRJobSuccess = (aprioriPassKMRJob.waitForCompletion(true) ? true : false);
		System.out.println("Finished AprioriPhase" + passNum +"Job");
		
		return isMRJobSuccess;
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
	public static class AprioriPass1Mapper extends Mapper<Object, Text, Text, IntWritable> {
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
			if(AprioriUtils.hasMinSupport(MIN_SUPPORT_PERCENT, MAX_NUM_TXNS, countItemId)) {
				context.write(new Text(itemsetIds), new IntWritable(countItemId));	
			}
		}
	}
	
	// Phase2 - MapReduce
	/*
	 * Mapper for PhaseK would emit a <itemId, 1> pair for each item across all transactions.
	 */
	public static class AprioriPassKMapper extends Mapper<Object, Text, Text, IntWritable> {
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

	public static void main(String[] args) throws Exception
	{
		int exitCode = ToolRunner.run(new MRApriori(), args);
		System.exit(exitCode);
	}
}
