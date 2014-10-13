package nl.arthurvlug.hadoop.numbersort;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class NumberSortMRUnitTest {
	private MapDriver<LongWritable, Text, OrderedText, NullWritable> mapDriver;
	private MapReduceDriver<LongWritable, Text, OrderedText, NullWritable, OrderedText, NullWritable> mapReduceDriver;

	@Before
	public void setup() {
		NumberSortMapper mapper = new NumberSortMapper();

		Configuration conf = new Configuration();
		
		mapDriver = new MapDriver<LongWritable, Text, OrderedText, NullWritable>(mapper);
		mapDriver.setMapper(mapper);
		mapDriver.setConfiguration(conf);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, OrderedText, NullWritable, OrderedText, NullWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(new Reducer<OrderedText, NullWritable, OrderedText, NullWritable>());
		mapReduceDriver.setConfiguration(conf);
	}

	@Test
	public void testNumberSort() throws ClassNotFoundException, IOException, InterruptedException {
		mapReduceDriver.withInput(new LongWritable(1), new Text("-80"));
		mapReduceDriver.withInput(new LongWritable(2), new Text("-72"));
		mapReduceDriver.withInput(new LongWritable(3), new Text("39"));
		mapReduceDriver.withInput(new LongWritable(4), new Text("-87"));
		mapReduceDriver.withInput(new LongWritable(5), new Text("4"));
		mapReduceDriver.withInput(new LongWritable(6), new Text("-31"));
		mapReduceDriver.withInput(new LongWritable(7), new Text("17"));
		mapReduceDriver.withInput(new LongWritable(8), new Text("80"));
		mapReduceDriver.withInput(new LongWritable(9), new Text("-27"));
		
		mapReduceDriver.withOutput(new OrderedText( new Text("-87")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("-80")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("-72")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("-31")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("-27")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("4")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("17")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("39")), NullWritable.get());
		mapReduceDriver.withOutput(new OrderedText( new Text("80")), NullWritable.get());
		mapReduceDriver.runTest();
	}

	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(0), new Text("-80"));
		mapDriver.withOutput(new OrderedText( new Text("-80")), NullWritable.get());
		mapDriver.runTest();
	}
}
