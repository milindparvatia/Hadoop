package mapReduce;

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Pairs {
    private static final Logger LOG = Logger.getLogger(Pairs.class);
    
    public static class PairsOccurrenceMapper extends Mapper<LongWritable, Text, WordPair, IntWritable> {
        private WordPair wordPair = new WordPair();
        private IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	LOG.setLevel(Level.DEBUG);
	        
	    	try {
		    	// Log every line
				LOG.setLevel(Level.DEBUG);
				LOG.debug("The mapper task of Milind Shaileshkumar Parvatia, s3806853");
				StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");

				if(tokenizer.countTokens() <= 100){
					ArrayList<String> tokens = new ArrayList<String>();
					while (tokenizer.hasMoreTokens()) {
						tokens.add(tokenizer.nextToken());
					}

					if (tokens.size() > 1) {
					  for (int i = 0; i < tokens.size(); i++) {
						 wordPair.setWord(tokens.get(i));

						  for (int j = 0; j < tokens.size(); j++) {
							   wordPair.setNeighbor(tokens.get(j));
							   context.write(wordPair, ONE);
						  }
					  }
					}
				}
	    	} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
	    	}
		}
      }

	public static class PairPartitioner extends Partitioner<WordPair,IntWritable> {
		//Partition Task
		public int getPartition(WordPair wordPair, IntWritable intWritable, int numPartitions) {
			LOG.setLevel(Level.INFO);
			LOG.info("The partition task of Milind Parvatia , S3806853");
			return Math.abs(wordPair.getWord().hashCode() % numPartitions);
		}
	}

    public static class PairsReducer extends Reducer<WordPair,IntWritable,WordPair,IntWritable> {
		private IntWritable totalCount = new IntWritable();
        @Override
        protected void reduce(WordPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			LOG.setLevel(Level.DEBUG);
			// Log every line
			LOG.debug("The reducer task of Milind Shaileshkumar Parvatia, s3806853");

			int count = 0;
            for (IntWritable value : values) {
                 count += value.get();
            }
            totalCount.set(count);
            context.write(key,totalCount);
        }
    }
    
    public static void main(String[] args) throws Exception {
    	// TODO Auto-generated method stub
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Pairs");
    	job.setJarByClass(Pairs.class);
    	job.setMapperClass(PairsOccurrenceMapper.class);
		job.setOutputKeyClass(WordPair.class);
		job.setOutputValueClass(IntWritable.class);
		job.setPartitionerClass(PairPartitioner.class);
		job.setNumReduceTasks(3);

    	job.setCombinerClass(PairsReducer.class);
    	job.setReducerClass(PairsReducer.class);

    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

