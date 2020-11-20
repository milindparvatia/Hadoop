package mapReduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Partitioner;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class PairsRelative extends Configured implements Tool {
	private static final Logger LOG = Logger.getLogger(PairsRelative.class);

	public static class PairsRelativeMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
		private static final Logger LOG_MAPPER = Logger.getLogger(PairsRelative.class);
		private WordPair wordPair = new WordPair();
	    private IntWritable ONE = new IntWritable(1);
	    private IntWritable totalCount = new IntWritable();

	    //Mapper Task
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				// Log every line
				LOG.debug("The mapper task of Milind Shaileshkumar Parvatia, s3806853");
				StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");

				if(tokenizer.countTokens() <= 100){
					ArrayList<String> tokens = new ArrayList<String>();
					while (tokenizer.hasMoreTokens()) {
						tokens.add(tokenizer.nextToken());
					}

					if (tokens.size() > 1) {
						int neighbors = context.getConfiguration().getInt("neighbors", tokens.size());
						for (int i = 0; i < tokens.size(); i++) {
							wordPair.setWord(tokens.get(i));

							int start = (i - neighbors < 0) ? 0 : i - neighbors;
							int end = (i + neighbors >= tokens.size()) ? tokens.size() - 1 : i + neighbors;
							for (int j = start; j <= end; j++) {
								if (j == i) continue;
								wordPair.setNeighbor(tokens.get(j).replaceAll("\\W",""));
								context.write(wordPair, ONE);
							}
							wordPair.setNeighbor("*");
							totalCount.set(end - start);
							context.write(wordPair, totalCount);
						}
	    			}
				}
			} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
			}
		}
	}



	public static class PairsRelativeReducer extends Reducer<WordPair, IntWritable, WordPair, DoubleWritable> {
		private static final Logger LOG = Logger.getLogger(PairsRelative.class);
		private DoubleWritable totalCount = new DoubleWritable();	    
		private DoubleWritable relativeCount = new DoubleWritable();	    
		private Text currentWord = new Text("NOT_SET");	    
		private Text flag = new Text("*");
		//Reducer Task
	    protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	    	LOG.setLevel(Level.INFO);
			LOG.debug("The reducer task of Milind Shaileshkumar Parvatia, s3806853");

			if (key.getNeighbor().equals(flag)) {
	    		if (key.getWord().equals(currentWord)) {	            
	    			totalCount.set(totalCount.get() + getTotalCount(values));	            
	    		} else {	            
	    			currentWord.set(key.getWord());	                
	    			totalCount.set(0);	                
	    			totalCount.set(getTotalCount(values));	            
	    		}	        
	    	} else {
	        	int count = getTotalCount(values);	            
	    		relativeCount.set((double) count / totalCount.get());            
	    		context.write(key, relativeCount);        
	    	}    	
	    }

	    private int getTotalCount(Iterable<IntWritable> values) {	        
	    	int count = 0;	        
	    	for (IntWritable value : values) {	        
	    		count += value.get();	        
	    	}	    	
	        return count;	    
	    }	
	}
    
	public static class WordPairPartitioner extends Partitioner<WordPair,IntWritable> {
		private static final Logger LOG = Logger.getLogger(WordPairPartitioner.class);
	    //Partition Task
	    public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
			LOG.setLevel(Level.INFO);
			LOG.debug("The partition task of Milind Shaileshkumar Parvatia, s3806853");
	    	return Math.abs(wordPair.getWord().hashCode() % numPartitions);	    	
	    }
	}

	public static void main(String[] args) throws Exception {
    	System.out.println("Started running PairsRelative job.");
		int res = ToolRunner.run(new PairsRelative(), args);
		//exiting the job only if the flag value becomes false
		System.exit(res);
   
    }

    public int run(String[] args) throws Exception {

    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "PairsRelative");
    	job.setJarByClass(PairsRelative.class);
    	job.setMapperClass(PairsRelativeMapper.class);
        job.setReducerClass(PairsRelativeReducer.class);
        job.setPartitionerClass(WordPairPartitioner.class);
        job.setNumReduceTasks(3);
     // Specify data type of output key and value
        job.setOutputKeyClass(WordPair.class);
        job.setOutputValueClass(IntWritable.class);
        FileSystem fs = FileSystem.get(conf);
		//checking if output path is already exists, then deleting it
		if(fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]),true);
		}
		//Configuring the input/output path from the file system into the job
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//Configuring the input/output path from the log
		LOG.info("Input path: " + args[0]);
		LOG.info("Output path: " + args[1]);
		return (job.waitForCompletion(true) ? 0 : 1);
    }
}
