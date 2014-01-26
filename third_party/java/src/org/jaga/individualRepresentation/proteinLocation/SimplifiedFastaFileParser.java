package org.jaga.individualRepresentation.proteinLocation;

import java.io.*;
import java.util.ArrayList;

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

public class SimplifiedFastaFileParser implements ProteinFileParser {

	public SimplifiedFastaFileParser() {}

	public void readFromFile(String fileName, ProteinGroup callbackAccumulator)
															 throws java.io.IOException {

		BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		try {

			String s = in.readLine();

			while (null != s) {
				while (null != s && (0 == s.trim().length() || '>' != s.trim().charAt(0)))
					s = in.readLine();

				if (null == s)
					continue;

				String name = "<untitled>";
				String sequence = "";
				s = s.trim();
				if (s.length() > 1)
					name = s.substring(1);

				s = in.readLine();
				while(s != null && s.trim().length() > 0 && '>' != s.trim().charAt(0)) {
					//System.out.println(s);
					sequence += s.trim().toUpperCase();
					s = in.readLine();
				}

				ArrayList seqs = new ArrayList();
				seqs.add(sequence);
				int i = 0;
				while (i < seqs.size()) {
					String seq = (String) seqs.get(i);
					if (seq.indexOf('B') >= 0) {
						seqs.remove(i);
						copyReplace(sequence, 'B', 'D');
						copyReplace(sequence, 'B', 'N');
						i = 0;
					} else if (seq.indexOf('Z') >= 0) {
						seqs.remove(i);
						copyReplace(sequence, 'Z', 'E');
						copyReplace(sequence, 'Z', 'Q');
						i = 0;
					} else
						++i;
				}

				if (1 == seqs.size()) {
					Protein prot = new Protein(name, (String) seqs.get(0));
					callbackAccumulator.add(prot);
				} else {
					for (i = 0; i < seqs.size(); i++) {
						Protein prot = new Protein(name + "(" + i + ")", (String) seqs.get(i));
						callbackAccumulator.add(prot);
					}
				}

			}

		} finally {
			in.close();
		}
	}

	private String copyReplace(String str, char oldChar, char newChar) {
		StringBuffer b = new StringBuffer(str);
		for (int i = 0; i < b.length(); i++) {
			if (b.charAt(i) == oldChar)
				b.setCharAt(i, newChar);
		}
		return b.toString();
	}
}