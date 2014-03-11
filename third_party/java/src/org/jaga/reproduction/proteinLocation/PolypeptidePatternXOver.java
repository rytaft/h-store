package org.jaga.reproduction.proteinLocation;

import org.jaga.reproduction.XOver;
import org.jaga.individualRepresentation.proteinLocation.*;
import java.util.Arrays;
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

public class PolypeptidePatternXOver extends XOver {

	private int xOverPoints = 1;

	public PolypeptidePatternXOver() {
	}

	public PolypeptidePatternXOver(double xOverProb) {
		super(xOverProb);
	}

	public PolypeptidePatternXOver(int xOverPoints) {
		super();
		this.xOverPoints = xOverPoints;
	}

	public PolypeptidePatternXOver(int xOverPoints, double xOverProb) {
		super(xOverProb);
		this.xOverPoints = xOverPoints;
	}

	public Individual[] reproduce(Individual[] parents, GAParameterSet params) {

		// copy parents:
		ProteinLocationClassifierFactory factory = (ProteinLocationClassifierFactory) params.getIndividualsFactory();
		Individual [] kids = new Individual [] {
							 factory.createSpecificIndividual(parents[0], params),
							 factory.createSpecificIndividual(parents[1], params)};

		// check if XOver happens:
		RandomGenerator rnd = params.getRandomGenerator();
		if (rnd.nextDouble() < getXOverProbability())
			return kids;

		// ger the patterns:
		PolypeptidePattern [] parP = new PolypeptidePattern [] {
									 ((ProteinLocationClassifier) kids[0]).getPattern(),
									 ((ProteinLocationClassifier) kids[1]).getPattern(),
								};
		int [] lens = new int [] {parP[0].getLength(), parP[1].getLength()};
		PolypeptidePattern [] kidP = new PolypeptidePattern [] {
									 new PolypeptidePattern(), new PolypeptidePattern()
								};

		// set the xOver points:
		int xPointCount = Math.min(lens[0] - 1, lens[1] - 1);
		xPointCount = Math.min(xPointCount, this.xOverPoints);
		if (1 > xPointCount)
			return kids;
		int [][] xPoints = new int[2][xPointCount + 1];
		for (int i = 0; i < 2; i++)
			for (int j = 0; j <= xPointCount; xPoints[i][j++] = -1);
		//System.err.println();
		for (int i = 0; i < 2; i++) {
			for (int j = 0; j < xPointCount; j++) {
				int xOver = rnd.nextInt(1, lens[i]);
				while (contains(xPoints[i], xOver))
					xOver = rnd.nextInt(1, lens[i]);

				//System.err.println(i + " / " + j + " / " + xOver +" / "+ lens[i]);
				xPoints[i][j] = xOver;
			}
			xPoints[i][xPointCount] = lens[i];
			Arrays.sort(xPoints[i]);
			//for (int x = 0; x < xPoints[i].length; System.err.print(" " + xPoints[i][x++]));
			//System.err.println();
		}

		// do the xOver:
		int tmp;
		int [] src = new int [] {0, 1};
		int [] srcInd = new int [] {0, 0};
		for (int p = 0; p <= xPointCount; p++) {
			for (int k = 0; k < 2; k++) {
				for (; srcInd[src[k]] < xPoints[src[k]][p]; srcInd[src[k]]++)
					kidP[k].insertItem(parP[src[k]].getItem(srcInd[src[k]]));
			}
			tmp = src[0];
			src[0] = src[1];
			src[1] = tmp;
			/*
			tmp = srcInd[0];
			srcInd[0] = srcInd[1];
			srcInd[1] = tmp;
			*/
		}


		if (kidP[0].getLength() <= factory.getMaxPatternLength()
				&& kidP[1].getLength() <= factory.getMaxPatternLength()) {
			for (int i = 0; i < 2; i++)
				((ProteinLocationClassifier) kids[i]).setPattern(kidP[i]);
		}

		return kids;
	}

	private boolean contains(int [] array, int positiveNum) {
		for (int i = 0; array[i] >= 0 && i < array.length; i++)
			if (array[i] == positiveNum)
				return true;
		return false;
	}

	public Class getApplicableClass() {
		return ProteinLocationClassifier.class;
	}

	public int getXOverPoints() {
		return xOverPoints;
	}

	public void setXOverPoints(int xOverPoints) {
		this.xOverPoints = xOverPoints;
	}
}