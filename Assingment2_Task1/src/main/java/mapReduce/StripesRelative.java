package mapReduce;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.*;


public class StripesRelative{

	private static final Logger LOG = Logger.getLogger(StripesRelative.class);
	
	public static class StripRelativeMapper extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				LOG.setLevel(Level.DEBUG);
				// Log every line
				LOG.debug("The mapper task of Milind Shaileshkumar Parvatia, s3806853");
				StringTokenizer tokenizer = new StringTokenizer(value.toString(), " ");

				if (tokenizer.countTokens() <= 100) {
					ArrayList<String> tokens = new ArrayList<String>();
					while (tokenizer.hasMoreTokens()) {
						tokens.add(tokenizer.nextToken());
					}

					for (String word : tokens) {
						if (word.matches("^\\w+$")) {
							Map<String, Integer> stripe = new HashMap<String, Integer>();

							for (String term : tokens) {
								if (term.matches("^\\w+$") && !term.equals(word)) {
									Integer count = stripe.get(term);
									stripe.put(term, (count == null ? 0 : count) + 1);
								}
							}

							StringBuilder stripeStr = new StringBuilder();
							for (Map.Entry entry : stripe.entrySet()) {
								stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
							}

							if (!stripe.isEmpty()) {
								context.write(new Text(word), new Text(stripeStr.toString()));
							}
						}
					}
				}
			} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
			}
		}
	}

    private static class StripRelativeCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            Map<String, Integer> stripe = new HashMap<String, Integer>();

            for (Text value : values) {
                String[] stripes = value.toString().split(",");

                for (String termCountStr : stripes) {
                    String[] termCount = termCountStr.split(":");
                    String term = termCount[0];
                    int count = Integer.parseInt(termCount[1]);

                    Integer countSum = stripe.get(term);
                    stripe.put(term, (countSum == null ? 0 : countSum) + count);
                }
            }

            StringBuilder stripeStr = new StringBuilder();
            for (Map.Entry entry : stripe.entrySet()) {
                stripeStr.append(entry.getKey()).append(":").append(entry.getValue()).append(",");
            }

            context.write(key, new Text(stripeStr.toString()));
        }
    }

    public static class StripsRelativeReducer extends Reducer<Text, Text, Text, Text> {
    	TreeSet<Pair> priorityQueue = new TreeSet<Pair>(); 

    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

    		LOG.setLevel(Level.INFO);
			LOG.info("The reducer task of Milind Shaileshkumar Parvatia, s3806853");
    		Map<String, Integer> stripe = new HashMap<String, Integer>();
    		double totalCount = 0;
			String keyStr = key.toString();

			for (Text value : values) {
				String[] stripes = value.toString().split(",");

				for (String neighbhourStr : stripes) {
					 String[] neighbhour = neighbhourStr.split(":");
					 String term = neighbhour[0];
					 int currentCount = Integer.parseInt(neighbhour[1]);
					 Integer neighbhourCount = stripe.get(term); // Get the current neighbour counts
					 // Aggregate the count
					 stripe.put(term, (neighbhourCount == null ? 0 : neighbhourCount) + currentCount);
					 totalCount += currentCount;
					}
			}

			for (Map.Entry<String, Integer> entry : stripe.entrySet()) {
				priorityQueue.add(new Pair(entry.getValue() / totalCount, keyStr, entry.getKey()));
				if (priorityQueue.size() > 100) { priorityQueue.pollFirst(); }
			}
    	}

    	protected void cleanup(Context context) throws IOException, InterruptedException {
    	while (!priorityQueue.isEmpty()) {
    		Pair pair = priorityQueue.pollLast();
    		context.write(new Text(pair.key + "-" + pair.value), new Text(pair.relativeFrequency + ""));
    		}
    	}


    	class Pair implements Comparable<Pair> {
    		double relativeFrequency;
    		String key;
    		String value;

    		Pair(double relativeFrequency, String key, String value) {
				this.relativeFrequency = relativeFrequency;
				this.key = key;
				this.value = value;
			}

			public int compareTo(Pair pair) {
				 if (this.relativeFrequency >= pair.relativeFrequency) {
					 return 1;
				 } else {
					 return -1;
				 }
    	 	}
    	}
    }

    public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Relative Stripes");
		job.setJarByClass(StripesRelative.class);
		LOG.setLevel(Level.INFO);
		LOG.info("Input path: " + args[0]);
		LOG.info("Output path: " + args[1]);
		job.setMapperClass(StripRelativeMapper.class);
		job.setCombinerClass(StripRelativeCombiner.class);
		job.setReducerClass(StripsRelativeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
    
    
}
