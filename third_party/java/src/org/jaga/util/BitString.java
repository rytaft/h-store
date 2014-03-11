package org.jaga.util;



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

public class BitString {

	private int length;
	private int [] content = null;

	private BitString() {
		throw new UnsupportedOperationException("Use BitString(int)");
	}

	public BitString(int length) {
		this.length = length;
		int bufSize = length / 32;
		if (0 != length % 32)
			++bufSize;
		content = new int[bufSize];
		for (int i = 0; i < bufSize; content[i++] = 0);
	}

	public BitString(final BitString toCopy) {
		this.length = toCopy.length;
		int bufSize = length / 32;
		if (0 != length % 32)
			++bufSize;
		this.content = new int[bufSize];
		System.arraycopy(toCopy.content, 0, this.content, 0, this.content.length);
	}

	public Object clone() {
		BitString copy = new BitString(this.length);
		System.arraycopy(this.content, 0, copy.content, 0, copy.content.length);
		return copy;
	}

	public int getLength() {
		return length;
	}

	public BitString substring(int from, int to) {
		int len = checkSubLength(from, to);
		BitString substring = new BitString(len);
		for (int i = from; i < to; i++)
			substring.setUnchecked(i - from, this.getUnchecked(i));
		return substring;
	}

	public boolean get(int index) {
		checkIndex(index);
		return getUnchecked(index);
	}

	protected boolean getUnchecked(int index) {
		int segment = index / 32;
		int offset = index % 32;
		int mask = 0x1 << (32 - offset - 1);
		return 0 != (content[segment] & mask);
	}

	public void set(int index, boolean value) {
		checkIndex(index);
		setUnchecked(index, value);
	}

	public void setUnchecked(int index, boolean value) {
		int segment = index / 32;
		int offset = index % 32;
		int mask = 0x1 << (32 - offset - 1);
		if (value)
			content[segment] |= mask;
		else
			content[segment] &= ~mask;
	}

	public void set(int from, int to, boolean value) {
		checkSubLength(from, to);
		for (int i = from; i < to; setUnchecked(i++, value));
	}

	public void set(int from, int to, BitString values) {
		if (values.getLength() == 0)
			throw new IllegalArgumentException("Length of values must be > 0");
		int len = checkSubLength(from, to);
		int iV = 0;
		for (int i = from; i < to; i++, iV++) {
			if (iV >= values.getLength())
				iV = 0;
			this.setUnchecked(i, values.getUnchecked(iV));
		}
	}

	public void flip(int index) {
		checkIndex(index);
		int segment = index / 32;
		int offset = index % 32;
		int mask = 0x1 << (32 - offset - 1);
		content[segment] ^= mask;
	}

	public String toString() {
		StringBuffer s = new StringBuffer(length);
		for (int i = 0; i < length; s.append(get(i++) ? 1 : 0));
		return s.toString();
	}

	protected int checkSubLength(int from, int to) throws IndexOutOfBoundsException {
		checkIndex(from);
		checkIndex(to - 1);
		int sublen = to - from;
		if (0 > sublen)
			throw new IllegalArgumentException("must have 'from' <= 'to', but has "
											   + from + " > " + to);
		return sublen;
	}

	protected void checkIndex(int index) throws IndexOutOfBoundsException {
		if (index < 0 || index >= this.length)
			throw new IndexOutOfBoundsException("index is " + index
												+ ", but must be in [0, "
												+ (this.length - 1) + "]");
	}
}
