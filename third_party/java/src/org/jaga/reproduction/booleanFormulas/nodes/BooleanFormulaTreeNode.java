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

abstract public class BooleanFormulaTreeNode {

	private OperatorNode parent = null;
	private int depth = 0;
	private int height = 1;
	private Long handle = new Long(0);

	public BooleanFormulaTreeNode() {}

	abstract public boolean evaluate(boolean [] parameters);

	abstract public int getRequiredChildrenNumber();

	abstract public Object clone();

	abstract public int recalcHeight();

	public int getHeight() {
		return height;
	}

	protected void setHeight(int val) {
		height = val;
	}

	public String toString() {
		return toString(true);
	}

	public String toString(boolean infix) {
		return toStringBuffer(infix).toString();
	}

	abstract public StringBuffer toStringBuffer(boolean infix);

	public OperatorNode getParent() {
		return this.parent;
	}

	protected void setParent(OperatorNode parent) {
		this.parent = parent;
	}

	public int getDepth() {
		return this.depth;
	}

	public void setDepth(int depth) {
		this.depth = depth;
	}

	public Long getHandle() {
		return this.handle;
	}

	public void setHandle(Long handle) {
		this.handle = handle;
	}

}