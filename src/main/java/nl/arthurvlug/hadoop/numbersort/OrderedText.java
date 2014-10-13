package nl.arthurvlug.hadoop.numbersort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

@NoArgsConstructor
@AllArgsConstructor
public class OrderedText implements WritableComparable<OrderedText> {
	private static final OrderedTextSortComparator COMPARATOR = new OrderedTextSortComparator();
	
	@Getter
	private Text text = new Text();

	public int compareTo(OrderedText other) {
		return COMPARATOR.compare(this, other);
	}
	
	@Override
	public boolean equals(Object obj) {
		OrderedText orderedText = (OrderedText) obj;
		return text.equals(orderedText.text);
	}

	@Override
	public String toString() {
		return text.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		text.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		text.write(out);
	}
}
