package org.jaga.reproduction.booleanFormulas.nodes;

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

public class TerminalNode extends BooleanFormulaTreeNode {

	private int terminalIndex = -1;

	public TerminalNode() {
		terminalIndex = -1;
	}

	public TerminalNode(int terminalIndex) {
		this.terminalIndex = terminalIndex;
	}

	public Object clone() {
		TerminalNode copy = new TerminalNode(this.terminalIndex);
		return copy;
	}

	public int recalcHeight() {
		final int height = 1;
		setHeight(height);
		OperatorNode parent = getParent();
		if (null != parent)
			parent.propagateNewHeight();
		return height;
	}

	public void setTerminalIndex(int terminalIndex) {
		if (0 <= this.terminalIndex)
			throw new UnsupportedOperationException("Terminal index is read-only");
		this.terminalIndex = terminalIndex;
	}

	public int getRequiredChildrenNumber() {
		return 0;
	}

	public boolean evaluate(boolean [] parameters) {
		if (terminalIndex < 0 || terminalIndex >= parameters.length)
			throw new IndexOutOfBoundsException("terminalIndex is " + terminalIndex
												+ " but parameters are from 0 to "
												+ parameters.length);
		return parameters[terminalIndex];
	}

	public StringBuffer toStringBuffer(boolean infix) {
		StringBuffer s = new StringBuffer("p");
		s.append(terminalIndex);
		return s;
	}

}