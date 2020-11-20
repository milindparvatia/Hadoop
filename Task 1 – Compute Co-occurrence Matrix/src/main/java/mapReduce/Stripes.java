package mapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class Stripes {
	private static final Logger LOG = Logger.getLogger(Stripes.class);
    
    public static class StripesOccurrenceMapper extends Mapper<LongWritable,Text,Text,MapWritable> {
        private MapWritable occurrenceMap = new MapWritable();
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	LOG.setLevel(Level.DEBUG);
	        
	    	try {
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
	                    word.set(tokens.get(i));
	                    occurrenceMap.clear();  
	                    
	                    for (int j = 0; j < tokens.size(); j++) {
	   	                	Text neighbor = new Text(tokens.get(j));
	                        
	   	                	if(occurrenceMap.containsKey(neighbor)){
	                           IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
	                           count.set(count.get()+1);
	                        }else{
	                            occurrenceMap.put(neighbor, new IntWritable(1));
	                        }
	   	                  }
	                  context.write(word, occurrenceMap);
	                }
	            }
				}
	    	} catch (Exception ex) {
				LOG.error("Caught Exception", ex);
	    	}
        	
        }
    }
    
    public static class StripesReducer extends Reducer<Text, MapWritable, Text, MapWritable> {
        private MapWritable incrementingMap = new MapWritable();

        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
			LOG.setLevel(Level.DEBUG);
			// Log every line
			LOG.debug("The reducer task of Milind Shaileshkumar Parvatia, s3806853");
            incrementingMap.clear();
            for (MapWritable value : values) {
                addAll(value);
            }
            context.write(key, incrementingMap);
        }


        private void addAll(MapWritable mapWritable) {
            Set<Writable> keys = mapWritable.keySet();
            for (Writable key : keys) {
                IntWritable fromCount = (IntWritable) mapWritable.get(key);
                if (incrementingMap.containsKey(key)) {
                    IntWritable count = (IntWritable) incrementingMap.get(key);
                    count.set(count.get() + fromCount.get());
                } else {
                    incrementingMap.put(key, fromCount);
                }
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
    	// TODO Auto-generated method stub
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "Stripes");
    	job.setJarByClass(Stripes.class);
    	job.setMapperClass(StripesOccurrenceMapper.class);
    	job.setCombinerClass(StripesReducer.class);
    	job.setReducerClass(StripesReducer.class);
    	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

