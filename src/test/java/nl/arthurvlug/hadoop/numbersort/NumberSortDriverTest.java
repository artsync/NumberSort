package nl.arthurvlug.hadoop.numbersort;

import static org.junit.Assert.assertEquals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

public class NumberSortDriverTest {
	private Configuration conf;
	private Path input;
	private Path output;
	private FileSystem fs;

	@Before
	public void setup() throws IOException {
		conf = new Configuration();
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");

		input = new Path("src/test/resources/input");
		output = new Path("target/output");

		fs = FileSystem.getLocal(conf);
		fs.delete(output, true);
	}

	@Test
	public void test() throws Exception {
		NumberSort numberSort = new NumberSort();
		numberSort.setConf(conf);

		int exitCode = numberSort.run(new String[] { input.toString(), output.toString() });
		assertEquals(0, exitCode);
		
		validateOuput();
	}

	private void validateOuput() throws IOException {
		InputStream in = null;
		try {
			in = fs.open(new Path("target/output/part-r-00000"));

			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			assertEquals("-46", br.readLine());
			assertEquals("-36", br.readLine());
			assertEquals("-36", br.readLine());
			assertEquals("-34", br.readLine());
			assertEquals("-32", br.readLine());
			assertEquals("1", br.readLine());
			assertEquals("6", br.readLine());
			assertEquals("12", br.readLine());
			assertEquals("33", br.readLine());
			assertEquals("36", br.readLine());
			assertEquals("37", br.readLine());
			assertEquals("43", br.readLine());
			assertEquals("43", br.readLine());
			assertEquals("51", br.readLine());
			assertEquals("54", br.readLine());
			assertEquals("67", br.readLine());

		} finally {
			IOUtils.closeStream(in);
		}
	}
}
