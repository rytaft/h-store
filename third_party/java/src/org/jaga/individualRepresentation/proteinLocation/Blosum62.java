package org.jaga.individualRepresentation.proteinLocation;

import java.util.*;
import java.io.*;

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

public class Blosum62 extends SubstitutionScoringMatrix {

	private static final String dataFile = "D:\\Courseworks\\4C58\\cw\\data\\blosum62.dat";
	private HashMap logScores = null;
	private HashMap actualScores = null;

	public Blosum62() {
		logScores = new HashMap(21);
		actualScores = new HashMap(21);
		BufferedReader in = null;
		try {
			in = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)));
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
		try {
			try {
				// Read headline:
				String head = in.readLine();
				StringTokenizer tok = new StringTokenizer(head);
				String [] names = new String[25];
				int i = 0;
				while (tok.hasMoreTokens()) {
					String name = tok.nextToken();
					names[i++] = name.trim();
				}
				// Read data lines:
				i = 0;
				String line = in.readLine();
				while (null != line) {
					//System.out.println("LINE " + i + ": " + line + " / " + names[i]);
					if (names[i].equalsIgnoreCase("B")         // dont know what these
							|| names[i].equalsIgnoreCase("Z")  // codes stand for.
							|| names[i].equalsIgnoreCase("X")) {
						i++;
						line = in.readLine();
						continue;
					}
					AminoAcid aminoAcid = null;
					if (!names[i].equalsIgnoreCase("*"))
						aminoAcid = AminoAcidFactory.getResidueByCode(names[i]);
					tok = new StringTokenizer(line);
					HashMap log = new HashMap(21);
					HashMap act = new HashMap(21);
					int j = 0;
					while (tok.hasMoreTokens()) {
						String num = tok.nextToken();
						//System.out.println(j + " / " + num);
						if (names[j].equalsIgnoreCase("B")         // dont know what these
								|| names[j].equalsIgnoreCase("Z")  // codes stand for.
								|| names[j].equalsIgnoreCase("X")) {
							j++;
							continue;
						}
						AminoAcid aa = null;
						if (!names[j].equalsIgnoreCase("*"))
							aa = AminoAcidFactory.getResidueByCode(names[j]);
						int p = Integer.parseInt(num);
						log.put(aa, new Integer(p));
						act.put(aa, new Double(Math.pow(2, p)));
						j++;
					}
					logScores.put(aminoAcid, log);
					actualScores.put(aminoAcid, act);
					++i;
					line = in.readLine();
				}
			} finally {
				in.close();
			}
		} catch (IOException e) {
			throw new RuntimeException(e.getMessage());
		}
	}

	public int getLogScore(AminoAcid aminoAcid1, AminoAcid aminoAcid2) {
		HashMap map = (HashMap) logScores.get(aminoAcid1);
		Integer val = (Integer) map.get(aminoAcid2);
		return val.intValue();
	}

	public double getActualScore(AminoAcid aminoAcid1, AminoAcid aminoAcid2) {
		return ((Double) ((HashMap) actualScores.get(aminoAcid1)).get(aminoAcid2)).doubleValue();
	}

	/* TEST: */
	public static void main(String[] args) {
		Blosum62 m = new Blosum62();
		String aas = "ARNDCQEGHILKMFPSTWYV*";
		System.out.println();
		for (int i = 0; i < 21; i++) {
			System.out.print(aas.substring(i, i + 1));
			System.out.print("\t");
		}
		System.out.println();
		for (int i = 0; i < 20; i++) {
			for (int j = 0; j < 20; j++) {
				System.out.print(m.getLogScore(aas.substring(i, i + 1), aas.substring(j, j + 1)));
				System.out.print("\t");
			}
			System.out.print(m.getLogScore(aas.substring(i, i + 1), null));
			System.out.print("\t");
			System.out.println();
		}
		for (int j = 0; j < 20; j++) {
			System.out.print(m.getLogScore(null, aas.substring(j, j + 1)));
			System.out.print("\t");
		}
		System.out.print(m.getLogScore((String) null, (String) null));
		System.out.print("\t");
		System.out.println();
	}

}