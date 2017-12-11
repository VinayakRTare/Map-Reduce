import java.io.IOException;
import java.io.StringReader;
import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;

public class AvgSalary {

	final static long DEFAULT_SPLIT_SIZE = 128 * 1024 * 1024;

	public static class avgMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override

		public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException {
			String str = value.toString();
			CSVReader R = new CSVReader(new StringReader(str));
			@SuppressWarnings("deprecation")
			String line = "";
			int male_count = 0;
			int female_count = 0;
			String[] ParsedLine = str.split(",");
			R.close();
			String Ckey = "";
			if (!(ParsedLine.equals(null))) {
				if (!(ParsedLine[69].equals("SEX")) && !(ParsedLine[69].equals(null))) {
					Ckey = ParsedLine[5] + "." + ParsedLine[69];
					c.write(new Text(Ckey), new Text(ParsedLine[72]));
				}
			}

		}

	}

	public static class avgReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context c) throws IOException, InterruptedException {
			int count = 0;
			long salary = 0;
			for (Text val : values) {
				if (!(val.toString().equals("")) && !(val.toString().equals("WAGP"))
						&& !(Integer.parseInt(val.toString()) == 0)) {
					salary += Long.parseLong(val.toString());
					count += 1;
				}
			}
			c.write(key, new Text("" + (double) salary / (double) count));
		}

	}

	public static void main(String[] args) throws IOException, ClassNotFoundException,

			InterruptedException {
		long tt = System.nanoTime();
		Configuration conf = new Configuration();
		Job j2 = new Job(conf);
		j2.setJobName("AvgSalary job");
		j2.setJarByClass(AvgSalary.class);
		// Mapper input and output
		j2.setMapOutputKeyClass(Text.class);
		j2.setMapOutputValueClass(Text.class);
		// Reducer input and output
		j2.setOutputKeyClass(Text.class);
		j2.setOutputValueClass(Text.class);
		// file input and output of the whole program
		j2.setInputFormatClass(TextInputFormat.class);
		j2.setOutputFormatClass(TextOutputFormat.class);
		// Set the mapper class
		j2.setMapperClass(avgMapper.class);
		// Set the reducer class

		j2.setReducerClass(avgReducer.class);
		j2.setNumReduceTasks(2);
		FileOutputFormat.setOutputPath(j2, new Path(args[1]));

		FileInputFormat.addInputPath(j2, new Path(args[0]));

		j2.waitForCompletion(true);
		long et = System.nanoTime();
		System.out.println((et - tt) + "  nanoseconds");

	}

}
