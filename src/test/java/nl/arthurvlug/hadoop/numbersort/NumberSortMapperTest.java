package nl.arthurvlug.hadoop.numbersort;

import static org.mockito.Mockito.verify;

import java.io.IOException;

import nl.arthurvlug.hadoop.numbersort.NumberSortMapper;
import nl.arthurvlug.hadoop.numbersort.OrderedText;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("unchecked")
public class NumberSortMapperTest {
	private static final NullWritable VALUE = NullWritable.get();
	private static final LongWritable inputKey = new LongWritable(1);
	
	private final NumberSortMapper numberSortMapper = new NumberSortMapper();
	
	@SuppressWarnings("rawtypes")
	private final Context context = Mockito.mock(Context.class);
	
	@Test
	public void test15() throws IOException, InterruptedException {
		Text inputValue = new Text("15");
		numberSortMapper.map(inputKey, inputValue, context);
		
		verify(context).write(new OrderedText(new Text("15")), VALUE);
	}
	
	@Test
	public void test0() throws IOException, InterruptedException {
		Text inputValue = new Text("0");
		numberSortMapper.map(inputKey, inputValue, context);

		verify(context).write(new OrderedText(new Text("0")), VALUE);
	}
	
	@Test
	public void test_1() throws IOException, InterruptedException {
		Text inputValue = new Text("-1");
		numberSortMapper.map(inputKey, inputValue, context);

		verify(context).write(new OrderedText(new Text("-1")), VALUE);
	}
	
	@Test
	public void test_15() throws IOException, InterruptedException {
		Text inputValue = new Text("-15");
		numberSortMapper.map(inputKey, inputValue, context);

		verify(context).write(new OrderedText(new Text("-15")), VALUE);
	}
	
	@Test
	public void test_100() throws IOException, InterruptedException {
		Text inputValue = new Text("-100");
		numberSortMapper.map(inputKey, inputValue, context);

		verify(context).write(new OrderedText(new Text("-100")), VALUE);
	}
	
	@Test
	public void test_MAX() throws IOException, InterruptedException {
		Text inputValue = new Text(Integer.MAX_VALUE + "");
		numberSortMapper.map(inputKey, inputValue, context);

		verify(context).write(new OrderedText(new Text("2147483647")), VALUE);
	}
}
