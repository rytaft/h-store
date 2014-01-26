package org.jaga.individualRepresentation.proteinLocation;

import java.util.StringTokenizer;
import org.jaga.definitions.*;

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

public class ProteinLocationClassifierFactory implements IndividualsFactory {

	private int maxPatternLength = 100;
	private int aminoAcidChance = 2;
	private int groupChance = 15;
	private int propertyChance = 15;
	private int singleGapChance = 2;
	private int maxGroupLength = 20;
	private String classifierName = "<untitled>";

	public ProteinLocationClassifierFactory() {}

	public ProteinLocationClassifierFactory(int maxPatternLength, int aminoAcidChance,
											int groupChance, int propertyChance,
											int singleGapChance, int maxGroupLength,
											String classifierName) {
		this.maxPatternLength = maxPatternLength;
		this.aminoAcidChance = aminoAcidChance;
		this.groupChance = groupChance;
		this.propertyChance = propertyChance;
		this.singleGapChance = singleGapChance;
		this.maxGroupLength = maxGroupLength;
		this.classifierName = classifierName;
	}

	public PolypeptidePattern createRandomPattern(GAParameterSet params) {
		int len = params.getRandomGenerator().nextInt(1, maxPatternLength + 1);
		return createRandomPattern(len, params);
	}

	public PolypeptidePattern createRandomPattern(int length, GAParameterSet params) {
		PolypeptidePattern pattern = new PolypeptidePattern();
		for (int i = 0; i < length; i++)
			pattern.insertItem(i, createRandomPatternItem(params));
		return pattern;
	}

	public PolypeptidePatternItem createRandomPatternItem(GAParameterSet params) {
		RandomGenerator rnd = params.getRandomGenerator();
		int dice = rnd.nextInt(0, aminoAcidChance + groupChance + propertyChance + singleGapChance);

		// amino acid:
		if (dice < aminoAcidChance) {
			return AminoAcidFactory.getRandomResidue(params);
		}

		// group:
		if (dice < aminoAcidChance + groupChance) {
			AminoAcidGroup group = new AminoAcidGroup();
			int gLen = rnd.nextInt(1, maxGroupLength + 1);
			for (int i = 0; i < gLen; i++)
				group.addResidue(AminoAcidFactory.getRandomResidue(params));
			return group;
		}

		// property:
		if (dice < aminoAcidChance + groupChance + propertyChance) {
			return AminoAcidPropertyFactory.getRandomProperty(params);
		}

		// single gap:
		if (dice < aminoAcidChance + groupChance + propertyChance + singleGapChance) {
			return SingleGap.getInstance();
		}

		throw new Error("Should never come to execute this line. Have fun debugging!");
	}

	public Individual createDefaultIndividual(GAParameterSet params) {
		PolypeptidePattern p = new PolypeptidePattern();
		p.insertItem(SingleGap.getInstance());
		return createSpecificIndividual(p, params);
	}

	public Individual createRandomIndividual(GAParameterSet params) {
		PolypeptidePattern p = createRandomPattern(params);
		p.insertItem(SingleGap.getInstance());
		return createSpecificIndividual(p, params);
	}

	public Individual createSpecificIndividual(Object init, GAParameterSet params) {
		if (null == init)
			throw new NullPointerException("Cannot have an null initialisation object.");

		if (init instanceof ProteinLocationClassifier)
			return createSpecificIndividual((ProteinLocationClassifier) init, params);

		if (init instanceof PolypeptidePattern)
			return createSpecificIndividual((PolypeptidePattern) init, params);

		if (init instanceof String)
			return createSpecificIndividual((String) init, params);

		throw new ClassCastException("Cannot have an initialisation object of class"
										   + init.getClass().getName());
	}

	public Individual createSpecificIndividual(ProteinLocationClassifier init, GAParameterSet params) {
		return createSpecificIndividual(init.getPattern(), params);
	}

	public Individual createSpecificIndividual(PolypeptidePattern init, GAParameterSet params) {
		PolypeptidePattern p = new PolypeptidePattern();
		for (int i = 0; i < init.getLength(); i++) {
			PolypeptidePatternItem oIt = init.getItem(i);
			PolypeptidePatternItem nIt;
			if (oIt instanceof AminoAcidGroup) {
				nIt = new AminoAcidGroup();
				for (int j = 0; j < ((AminoAcidGroup) oIt).size(); j++)
					((AminoAcidGroup) nIt).addResidue(((AminoAcidGroup) oIt).getResidue(j));
			} else
				nIt = oIt;
			p.insertItem(nIt);
		}
		return new ProteinLocationClassifier(p, classifierName);
	}

	public Individual createSpecificIndividual(String init, GAParameterSet params) {
		PolypeptidePattern pattern = new PolypeptidePattern();
		StringTokenizer tok = new StringTokenizer(init, "-");
		while (tok.hasMoreTokens()) {
			String str = tok.nextToken();

			if (str.charAt(0) == '?') {
				pattern.insertItem(SingleGap.getInstance());
				//System.out.println("Parsed: " + pattern.getItem(pattern.getLength()-1));
				continue;
			}

			if (str.charAt(0) == '<') {
				String s = str.substring(1, str.length() - 1);
				pattern.insertItem(AminoAcidPropertyFactory.getPropertyByName(s));
				//System.out.println("Parsed: " + pattern.getItem(pattern.getLength()-1));
				continue;
			}

			if (Character.isLetter(str.charAt(0))) {
				pattern.insertItem(AminoAcidFactory.getResidueByCode(str.substring(0, 1)));
				//System.out.println("Parsed: " + pattern.getItem(pattern.getLength()-1));
				continue;
			}

			if (str.charAt(0) == '[') {
				AminoAcidGroup g = new AminoAcidGroup();
				for (int p = 1; str.charAt(p) != ']'; p++)
					g.addResidue(AminoAcidFactory.getResidueByCode(str.substring(p, p+1)));
				pattern.insertItem(g);
				//System.out.println("Parsed: " + pattern.getItem(pattern.getLength()-1));
				continue;
			}

			throw new RuntimeException("Parse error!");
		}
		return createSpecificIndividual(pattern, params);
	}

	public int getAminoAcidChance() {
		return aminoAcidChance;
	}

	public void setAminoAcidChance(int aminoAcidChance) {
		this.aminoAcidChance = aminoAcidChance;
	}

	public int getGroupChance() {
		return groupChance;
	}

	public void setGroupChance(int groupChance) {
		this.groupChance = groupChance;
	}

	public int getMaxGroupLength() {
		return maxGroupLength;
	}

	public void setMaxGroupLength(int maxGroupLength) {
		this.maxGroupLength = maxGroupLength;
	}

	public int getMaxPatternLength() {
		return maxPatternLength;
	}

	public void setMaxPatternLength(int maxPatternLength) {
		this.maxPatternLength = maxPatternLength;
	}

	public int getPropertyChance() {
		return propertyChance;
	}

	public void setPropertyChance(int propertyChance) {
		this.propertyChance = propertyChance;
	}

	public int getSingleGapChance() {
		return singleGapChance;
	}

	public void setSingleGapChance(int singleGapChance) {
		this.singleGapChance = singleGapChance;
	}

	public String getClassifierName() {
		return classifierName;
	}

	public void setClassifierName(String classifierName) {
		this.classifierName = classifierName;
	}

}