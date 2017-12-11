import java.io.IOException;
import java.io.StringReader;
import com.opencsv.CSVReader;
import org.apache.hadoop.conf.Configuration;
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

public class Histogram {

	public static class histMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override

		public void map(LongWritable key, Text value, Context c) throws

		IOException, InterruptedException {

			String str = value.toString();
			CSVReader R = new CSVReader(new StringReader(str));

			@SuppressWarnings("deprecation")
			String line = "";
			String ckey = "";
			String[] ParsedLine = R.readNext();
			R.close();

			String year = org.apache.commons.lang.StringUtils.substring(ParsedLine[0],0, 4);
			if (!(ParsedLine[8].equals("")) && !(ParsedLine[8].equals("AGEP")) && !(ParsedLine[0].equals("")))
			{
				if (0 <= Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 10)
				{
					ckey = year.concat(" 0-9");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (9 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 20)
				{
					ckey = year.concat(" 10-19");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (19 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 30)
				{
					ckey = year.concat(" 20-29");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (29 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 40)
				{
					ckey = year.concat(" 30-39");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (39 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 50)
				{
					ckey = year.concat(" 40-49");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (49 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 60)
				{
					ckey = year.concat(" 50-59");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (59 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 70)
				{
					ckey = year.concat(" 60-69");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (69 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 80)
				{
					ckey = year.concat(" 70-79");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (79 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 90)
				{
					ckey = year.concat(" 80-89");
					c.write(new Text(ckey), new Text("1"));
				}
				else if (89 < Integer.parseInt(ParsedLine[8]) && Integer.parseInt(ParsedLine[8]) < 100)
				{
					ckey = year.concat(" 90-99");
					c.write(new Text(ckey), new Text("1"));
				}
			}
		}
	}
	
	public static class histReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context c) throws
		IOException, InterruptedException {
			int count = 0;
			for (Text val : values) {
				count += 1;
			}
			c.write(key, new Text("," + count));
		}
	}
	public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
		Configuration conf = new Configuration();
		// conf.setInt("mapred.tasktracker.map.task.maximum", 3);
		Job j2 = new Job(conf);
		j2.setJobName("Histogram job");
		j2.setJarByClass(Histogram.class);
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
		j2.setMapperClass(histMapper.class);
		// set the combiner class for custom combiner
		// Set the reducer class
		j2.setReducerClass(histReducer.class);
		FileOutputFormat.setOutputPath(j2, new Path(args[1]));
		FileInputFormat.addInputPath(j2, new Path(args[0]));
		j2.waitForCompletion(true);

	}

}
