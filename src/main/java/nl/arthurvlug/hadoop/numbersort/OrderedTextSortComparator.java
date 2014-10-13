package nl.arthurvlug.hadoop.numbersort;

import org.apache.hadoop.io.Text;

public class OrderedTextSortComparator {

	public int compare(OrderedText o1, OrderedText o2) {
		Text text1 = o1.getText();
		Text text2 = o2.getText();
		
		boolean negative1 = text1.getBytes()[0] == '-';
		boolean negative2 = text2.getBytes()[0] == '-';
		
		if(negative1 && !negative2) {
			return -1;
		} else if(!negative1 && negative2) {
			return 1;
		} else if(negative1 && negative2) {
			return -compareSameSign(text1, text2);
		} else {
			return compareSameSign(text1, text2);
		}
	}

	private int compareSameSign(Text text1, Text text2) {
		int lengthDiff = text1.getLength() - text2.getLength();
		if(lengthDiff != 0) {
			return lengthDiff;
		}
		
		return text1.compareTo(text2);
	}

}
