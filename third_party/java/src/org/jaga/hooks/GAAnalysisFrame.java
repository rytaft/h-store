package org.jaga.hooks;


import javax.swing.*;
import java.awt.*;
import java.awt.event.*;
import org.jaga.util.FittestIndividualResult;


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

public class GAAnalysisFrame extends JFrame {

	private JPanel buttonPanel = new JPanel();
	private JButton closeButton = new JButton();
	private JSplitPane splitPane = new JSplitPane();
	private JTextArea statsText = new JTextArea();
	private JPanel graphPanel = null;

	private AnalysisHook hook = null;
	private AnalysisFrameUpdateThread updateThread = null;

	private GAAnalysisFrame() {
		throw new UnsupportedOperationException("Use GAAnalysisFrame(AnalysisHook hook) instead");
	}

	public GAAnalysisFrame(AnalysisHook hook) {
		super();
		if (null == hook)
			throw new NullPointerException("Analysis hook may not be null");
		this.hook = hook;
		graphPanel = new AnalysisGraphPanel(hook);
		try {
			jbInit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		this.setTitle(hook.getPlotFrameTitle());
		long delay = hook.getUpdateDelay();
		if (delay > 0) {
			updateThread = new AnalysisFrameUpdateThread(delay, this, hook);
			updateThread.start();
		} else {
			updateThread = null;
		}
	}

	private void jbInit() throws Exception {
		buttonPanel.setMinimumSize(new Dimension(10, 10));
	buttonPanel.setPreferredSize(new Dimension(550, 35));
		closeButton.setMaximumSize(new Dimension(120, 25));
		closeButton.setMinimumSize(new Dimension(130, 25));
		closeButton.setPreferredSize(new Dimension(130, 25));
		closeButton.setText("Close window");
		closeButton.addActionListener(new java.awt.event.ActionListener() {
			public void actionPerformed(ActionEvent e) {
				closeButton_actionPerformed(e);
			}
		});
		splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
		splitPane.setBorder(null);
		splitPane.setDebugGraphicsOptions(0);
		statsText.setBackground(SystemColor.info);
		statsText.setMinimumSize(new Dimension(6, 50));
		statsText.setOpaque(true);
		statsText.setPreferredSize(new Dimension(550, 230));
		statsText.setCaretPosition(0);
		statsText.setEditable(false);
		statsText.setText("Statistics:");
		graphPanel.setMinimumSize(new Dimension(10, 50));
		graphPanel.setPreferredSize(new Dimension(550, 250));
		this.setTitle("Genetic Algorithm Analysis");
		this.getContentPane().add(buttonPanel, BorderLayout.SOUTH);
		buttonPanel.add(closeButton, null);
		this.getContentPane().add(splitPane, BorderLayout.CENTER);
		splitPane.add(statsText, JSplitPane.BOTTOM);
		splitPane.add(graphPanel, JSplitPane.TOP);
		splitPane.setDividerLocation(300);

		this.setDefaultCloseOperation(DISPOSE_ON_CLOSE);
		this.setLocation(50, 50);
	}

	void closeButton_actionPerformed(ActionEvent e) {
		hide();
		dispose();
	}

	public void dispose() {
		if (null != updateThread) {
			updateThread.quit();
			updateThread = null;
		}
		hook.frameDisposed();
		super.dispose();
	}

	public void updateView() {
		updateTextDisplay();
		graphPanel.repaint();
		hook.viewUpdateComplete();
	}

	private void updateTextDisplay() {
		StringBuffer out = new StringBuffer("STATISTICS:");
		if (hook.isAnalyseGenAge()) {
			out.append("\nGeneration: ");
			out.append(hook.getGenerationNum());
		}
		if (hook.isAnalyseTotalFitEvals()) {
			out.append("\nFitness evaluations: ");
			out.append(hook.getFitnessCalculations());
		}
		if (hook.isAnalyseRunTime()) {
			out.append("\nRun time: ");
			out.append(hook.getRunTime());
		}
		if (hook.isAnalyseBestFitness()) {
			FittestIndividualResult r = hook.getBestResult();
			if (null != r) {
				out.append("\nCurrently best result: ");
				out.append(r.toString());
			}
			out.append("\n[BLUE] Best fitness to date: ");
			out.append(hook.getBestFitnessValue(hook.getBestFitnessValueCount() - 1).toStrBuf());
		}
		if (hook.isAnalyseGenMinFit()) {
			out.append("\n[RED] Minimal fitness in last generation: ");
			out.append(hook.getMinFitness(hook.getMinFitnessCount() - 1).toStrBuf());
		}
		if (hook.isAnalyseGenMaxFit()) {
			out.append("\n[GREEN] Maximal fitness in last generation: ");
			out.append(hook.getMaxFitness(hook.getMaxFitnessCount() - 1).toStrBuf());
		}
		if (hook.isAnalyseGenAverageFit()) {
			out.append("\n[BLACK] Average fitness of the last generation: ");
			out.append(hook.getAverageFitness(hook.getAverageFitnessCount() - 1).toStrBuf());
		}
		if (hook.isAnalyseGenFitStdDeviation()) {
			int g = hook.getAverageFitness(hook.getAverageFitnessCount() - 1).getGeneration();
			out.append("\n[ORANGE] Standard deviation of fitness in the last generation: ");
			out.append(hook.getStdDeviation(g));
		}
		Runtime rt = Runtime.getRuntime();
		out.append("\nMemory: free: ");
		out.append(rt.freeMemory() / 1024);
		out.append(" KB, total: ");
		out.append(rt.totalMemory() / 1024);
		out.append(" KB, max: ");
		out.append(rt.maxMemory() / 1024);
		out.append(" KB.");


		statsText.setText(out.toString());
	}

}