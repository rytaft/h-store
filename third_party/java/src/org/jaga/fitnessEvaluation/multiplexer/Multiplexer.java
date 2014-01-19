package org.jaga.fitnessEvaluation.multiplexer;


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

public class Multiplexer {

	private int dataLines = 4;
	private int ctrlLines = 2;
	private int totalLines = 6;

	public Multiplexer() {}

	public Multiplexer(int dataLines) {
		this.dataLines = dataLines;
		this.ctrlLines = calculateCtrlLines();
		this.totalLines = (this.dataLines + this.ctrlLines);
	}

	private int calculateCtrlLines() {
		double log = Math.log(dataLines) / Math.log(2); // i.e. log(2, dL)
		if (((int) log) == log)
			return (int) log;
		else
			throw new IllegalArgumentException("Multiplexer must contain 2^n data-lines, "
											   + "where n - Integer. " + dataLines
											   + " data-lines does not satisfy this condition");
	}

	// Input: [cN, ..., c1, c0, d0, d1, d2, ..., dN]
	public boolean evaluate(boolean [] input) {
		if (input.length != totalLines) {
			throw new IllegalArgumentException("Input has " + input.length
											   + " values, but it must contain "
											   + totalLines
											   + " values (" + ctrlLines
											   + " control lines + "
											   + dataLines + " data lines)");
		}
		int factor = 1;
		int line = 0;
		for (int i = ctrlLines - 1; i >= 0; i--, factor <<= 1) {
			if (input[i])
				line += factor;
		}
		boolean value = input[ctrlLines + line];
		return value;
	}

	public int getDataLines() {
		return dataLines;
	}

	public int getCtrlLines() {
		return ctrlLines;
	}
	public int getTotalLines() {
		return totalLines;
	}

	/*
	//TEST:
	public static void main(String[] args) {
		Multiplexer multi = new Multiplexer(4);
		System.out.println("[0, 0, 1, 0, 0, 0] => "
						   + multi.evaluate(new boolean[] {false, false, true, false, false, false}));
		System.out.println("[0, 1, 1, 0, 0, 0] => "
						   + multi.evaluate(new boolean[] {false, true, true, false, false, false}));
		System.out.println("[1, 1, 0, 0, 0, 1] => "
						   + multi.evaluate(new boolean[] {true, true, false, false, false, true}));
	}
	*/

}