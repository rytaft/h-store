package org.jaga.individualRepresentation.greycodedNumbers;

import org.jaga.definitions.*;
import org.jaga.util.*;


/**
 * TODO: Complete these comments.
 *
 * <p><u>Project:</u> JAGA - Java API for Genetic Algorithms.</p>
 *
 * <p><u>Company:</u> University College London and JAGA.Org
 *    (<a href="http://www.jaga.org" target="_blank">http://www.jaga.org</a>).
 * </p>
 *
 * <p><u>Copyright:</u> (c) 2004 by G. Paperin.<br/>
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation, ONLY if you include a note of the original
 *    author(s) in any redistributed/modified copy.<br/>
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.<br/>
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *    or see http://www.gnu.org/licenses/gpl.html</p>
 *
 * @author Greg Paperin (greg@jaga.org)
 *
 * @version JAGA public release 1.0 beta
 */

public class NNumbersGreycodedIndivudual implements BinaryEncodedIndividual {

	private BitString representation = null;
	private int size = -1;
	private Fitness fitness = null;
	private int precision = 64;

	private NNumbersGreycodedIndivudual() {
		throw new java.lang.UnsupportedOperationException("Dont use the default constructor!");
	}

	public NNumbersGreycodedIndivudual(int size, int precision) {
		if (1 > size)
			throw new IllegalArgumentException("Creating an empty individual");
		if (1 > precision)
			throw new IllegalArgumentException("Individual cannot us zero length representation");
		this.size = size;
		this.precision = precision;
		this.representation = new BitString(size * precision);
	}

	public int getPrecision() {
		return this.precision;
	}

	public int getSize() {
		return this.size;
	}

	public BitString getBitStringRepresentation() {
		return representation;
	}

	public void setBitStringRepresentation(BitString bits) {
		if (bits.getLength() != this.representation.getLength())
			throw new IllegalArgumentException("Length of the bit string is incorrect (is "
											   + bits.getLength() + " but expected "
											   + this.representation.getLength() + ")");
		representation = bits;
	}

	public Fitness getFitness() {
		return fitness;
	}

	public void setFitness(Fitness fitness) {
		this.fitness = fitness;
	}

	protected BitString getClearBitCode(int valueIndex) {
		if (valueIndex < 0 || valueIndex > getSize())
			throw new IndexOutOfBoundsException("value index is " + valueIndex
												+ ", but must be in [0, "
												+ getSize() + "]");
		BitString valRep = representation.substring(valueIndex * precision, (valueIndex+1) * precision);
		BitString clear = grey2bin(valRep);
		return clear;
	}

	protected void setClearBitCode(int valueIndex, BitString clear) {
		if (valueIndex < 0 || valueIndex > getSize())
			throw new IndexOutOfBoundsException("value index is " + valueIndex
												+ ", but must be in [0, "
												+ getSize() + "]");
		BitString grey = bin2grey(clear);
		representation.set(valueIndex * precision, (valueIndex+1) * precision, grey);
	}

	protected BitString grey2bin(BitString grey) {
		int len = grey.getLength();
		if (len < 2)
			return grey;
		BitString bin = new BitString(len);
		bin.set(0, grey.get(0));
		for (int i = 1; i < len; i++)
			bin.set(i, bin.get(i-1) ^ grey.get(i));
		return bin;
	}

	protected BitString bin2grey(BitString bin) {
		int len = bin.getLength();
		if (len < 2)
			return bin;
		BitString grey = new BitString(len);
		grey.set(0, bin.get(0));
		for (int i = 1; i < len; i++)
			grey.set(i, bin.get(i-1) ^ bin.get(i));
		return grey;
	}

/*
	 // TEST:
	public static void main(String[] args) {
		BitString s1 = new BitString(4);
		s1.set(0, false); s1.set(1, false); s1.set(2, false); s1.set(3, false);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, false); s1.set(2, false); s1.set(3, true);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, false); s1.set(2, true); s1.set(3, false);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, false); s1.set(2, true); s1.set(3, true);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, true); s1.set(2, false); s1.set(3, false);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, true); s1.set(2, false); s1.set(3, true);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, true); s1.set(2, true); s1.set(3, false);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, false); s1.set(1, true); s1.set(2, true); s1.set(3, true);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();

		s1.set(0, true); s1.set(1, false); s1.set(2, false); s1.set(3, false);
		System.out.println(s1);
		System.out.println(bin2grey(s1));
		System.out.println(grey2bin(bin2grey(s1)));
		System.out.println();
	}
	*/

}