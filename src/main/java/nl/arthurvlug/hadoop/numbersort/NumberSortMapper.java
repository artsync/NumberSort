package nl.arthurvlug.hadoop.numbersort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NumberSortMapper extends Mapper<LongWritable, Text, OrderedText, NullWritable> {
	private static final NullWritable VALUE = NullWritable.get();

	public void map(LongWritable inputkey, Text inputValue, Context context) throws IOException, InterruptedException {
		context.write(new OrderedText(inputValue), VALUE);
	}

}
