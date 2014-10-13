package nl.arthurvlug.hadoop.numbersort;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import nl.arthurvlug.hadoop.numbersort.OrderedText;

import org.apache.hadoop.io.Text;
import org.junit.Test;

public class OrderedTextTest {
	private static final double EPSILON = 0.000001;

	@Test
	public void zeroZero() {
		OrderedText o1 = new OrderedText(new Text("0"));
		OrderedText o2 = new OrderedText(new Text("0"));
		assertEquals(0, Math.signum(o1.compareTo(o2)), EPSILON);
	}

	@Test
	public void posPos() {
		OrderedText o1 = new OrderedText(new Text("2"));
		OrderedText o2 = new OrderedText(new Text("50"));
		assertEquals(-1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void posPosHundred() {
		OrderedText o1 = new OrderedText(new Text("2"));
		OrderedText o2 = new OrderedText(new Text("80"));
		assertEquals(-1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void negNeg() {
		OrderedText o1 = new OrderedText(new Text("-4"));
		OrderedText o2 = new OrderedText(new Text("-60"));
		assertEquals(1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void posHundredNegTen() {
		OrderedText o1 = new OrderedText(new Text("100"));
		OrderedText o2 = new OrderedText(new Text("-40"));
		assertEquals(1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void posNegHundred() {
		OrderedText o1 = new OrderedText(new Text("5"));
		OrderedText o2 = new OrderedText(new Text("-640"));
		assertEquals(1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void negTenPos() {
		OrderedText o1 = new OrderedText(new Text("-61"));
		OrderedText o2 = new OrderedText(new Text("8"));
		assertEquals(-1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void negPosTen() {
		OrderedText o1 = new OrderedText(new Text("-3"));
		OrderedText o2 = new OrderedText(new Text("53"));
		assertEquals(-1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void negTenNegHundred() {
		OrderedText o1 = new OrderedText(new Text("-55"));
		OrderedText o2 = new OrderedText(new Text("-87"));
		assertEquals(1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void negTwoNegTen() {
		OrderedText o1 = new OrderedText(new Text("-2"));
		OrderedText o2 = new OrderedText(new Text("-10"));
		assertEquals(1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void negOneNegTen() {
		OrderedText o1 = new OrderedText(new Text("-1"));
		OrderedText o2 = new OrderedText(new Text("-10"));
		assertEquals(1, Math.signum(o1.compareTo(o2)), EPSILON);
	}
	
	@Test
	public void testOrder() throws IOException, InterruptedException {
		List<OrderedText> list = Arrays.asList(
				new OrderedText(new Text("1")),
				new OrderedText(new Text("2")),
				new OrderedText(new Text("0")),
				new OrderedText(new Text("6")),
				new OrderedText(new Text("2")),
				new OrderedText(new Text("2")),
				new OrderedText(new Text("16")),
				new OrderedText(new Text("67")),
				new OrderedText(new Text("32")),
				new OrderedText(new Text("-4")),
				new OrderedText(new Text("-100")),
				new OrderedText(new Text("-10")),
				new OrderedText(new Text("0"))
		);
		Collections.sort(list);
		
		List<OrderedText> expectedList = Arrays.asList(
				new OrderedText(new Text("-100")),
				new OrderedText(new Text("-10")),
				new OrderedText(new Text("-4")),
				new OrderedText(new Text("0")),
				new OrderedText(new Text("0")),
				new OrderedText(new Text("1")),
				new OrderedText(new Text("2")),
				new OrderedText(new Text("2")),
				new OrderedText(new Text("2")),
				new OrderedText(new Text("6")),
				new OrderedText(new Text("16")),
				new OrderedText(new Text("32")),
				new OrderedText(new Text("67"))
		);
		
		assertEquals(expectedList, list);
	}
}
