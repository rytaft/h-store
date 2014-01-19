package org.jaga.individualRepresentation.proteinLocation;


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

public class Protein {

	private String name = null;
	private AminoAcid [] sequence = null;

	public Protein() {
		this.name = null;
		this.sequence = new AminoAcid[0];
	}

	public Protein(String name, String sequence) {
		this.name = name;
		sequence = sequence.trim();
		this.sequence = new AminoAcid[sequence.length()];
		for (int i = 0; i < sequence.length(); i++) {
			String code = sequence.substring(i, i + 1);
			AminoAcid aminoAcid = AminoAcidFactory.getResidueByCode(code);
			if (null == aminoAcid)
				throw new IllegalArgumentException("Could not construct an amino acid with the code '"
												 + code + "'.");
			this.sequence[i] = aminoAcid;
		}
	}

	public Protein(String name, AminoAcid [] sequence) {
		this.name = name;
		this.sequence = new AminoAcid[sequence.length];
		System.arraycopy(sequence, 0, this.sequence, 0, sequence.length);
	}

	public String getName() {
		return this.name;
	}

	public String getSequenceString() {
		StringBuffer s = new StringBuffer(sequence.length);
		for (int i = 0; i < sequence.length; i++)
			s.append(sequence[i].toString());
		return s.toString();
	}

	public AminoAcid [] getSequence() {
		AminoAcid [] seq = new AminoAcid[sequence.length];
		System.arraycopy(this.sequence, 0, seq, 0, this.sequence.length);
		return seq;
	}

	public AminoAcid [] getSequenceReferencePerformanceHack() {
		return sequence;
	}

	public AminoAcid getResidue(int index) {
		return this.sequence[index];
	}

	public int getLength() {
		return sequence.length;
	}

	/*
	public Protein.Iterator getIterator() {
		return new Protein.Iterator(this, 0);
	}

	public Protein.Iterator getIterator(int startIndex) {
		return new Protein.Iterator(this, startIndex);
	}
	*/

	public String toString() {
		StringBuffer s;
		if (null != name) {
			s = new StringBuffer(name);
			s.append(" : [");
		} else {
			s = new StringBuffer("[");
		}
		s.append(getSequenceString());
		s.append("]");
		return s.toString();
	}

	/*
	public class Iterator implements java.util.Iterator {

		private Protein prot = null;
		private int index = 0;

		private Iterator() {
			throw new UnsupportedOperationException("Use Iterator(Protein prot).");
		}

		private Iterator(Protein prot, int startIndex) {
			this.prot = prot;
			this.index = startIndex;
		}

		public boolean hasNext() {
			return index < prot.getLength();
		}

		public Object next() {
			return prot.getResidue(index++);
		}

		public AminoAcid nextResidue() {
			return prot.getResidue(index++);
		}

		public void remove() {
			throw new UnsupportedOperationException("Protein.Iterator cannot remove residues.");
		}
	}
	*/
}