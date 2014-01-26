package org.jaga.hooks;

import java.awt.*;
import javax.swing.JPanel;
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

public class AnalysisGraphPanel extends JPanel {

	private static final int border = 10;//pixels

	private AnalysisHook hook = null;
	private double xFactor = 0;
	private double yFactor = 0;

	private AnalysisGraphPanel() {
		throw new UnsupportedOperationException("Use AnalysisGraphPanel(AnalysisHook hook) instead");
	}

	public AnalysisGraphPanel(AnalysisHook hook) {
		super();
		if (null == hook)
			throw new NullPointerException("Analysis hook may not be null");
		setBackground(Color.white);
		this.hook = hook;
	}

	public void paint(Graphics g) {
		super.paint(g);
		if (null == hook || 0 == hook.getMaxPlotCount())
			return;

		synchronized (hook) {

			xFactor = (double) (getWidth() - 2 * border)
					  / (double) (hook.getMaxPlotCount() - 1);

			if (hook.getMaxPlotVal() == hook.getMinPlotVal())
				yFactor = 0;
			else
				yFactor = (double) (getHeight() - 2 * border)
						  / (hook.getMaxPlotVal() - hook.getMinPlotVal());

			plotScale(g, Color.LIGHT_GRAY);

			if (hook.isAnalyseGenMinFit())
				plotFitnessGraph(g, hook.getGenMinFitnesses(), Color.RED);

			if (hook.isAnalyseGenFitStdDeviation())
				plotStdDeviationGraph(g, hook.getGenAverageFitnesses(), Color.ORANGE);

			if (hook.isAnalyseGenAverageFit())
				plotFitnessGraph(g, hook.getGenAverageFitnesses(), Color.BLACK);

			if (hook.isAnalyseGenMaxFit())
				plotFitnessGraph(g, hook.getGenMaxFitnesses(), Color.GREEN);

			if (hook.isAnalyseBestFitness())
				plotBestGraph(g, hook.getBestFitnessValues(), Color.BLUE);
		}
	}

	private void plotScale(Graphics g, Color col) {

		g.setColor(col);

		int x1 = scaleX(0);
		int y1 = scaleY(0.5 * (hook.getMaxPlotVal() + hook.getMinPlotVal()));
		int x2 = scaleX(hook.getMaxPlotCount() - 1);
		int y2 = y1;
		g.drawLine(x1, y1, x2, y2);

		x1 = scaleX(0.5 * (double) (hook.getMaxPlotCount() - 1));
		y1 = scaleY(hook.getMaxPlotVal());
		x2 = x1;
		y2 = scaleY(hook.getMinPlotVal());
		g.drawLine(x1, y1, x2, y2);

		String s = (new StringBuffer()).append("(gen")
				   .append(0.5 * (double) (hook.getMaxPlotCount() - 1))
				   .append(", fit=")
				   .append(hook.getMaxPlotVal())
				   .append(")").toString();
		x1 = scaleX(0.5 * (double) (hook.getMaxPlotCount() - 1)) + 3;
		y1 = scaleY(hook.getMaxPlotVal()) + g.getFontMetrics().getHeight();
		g.drawString(s, x1, y1);

		s = (new StringBuffer()).append("(gen=")
				   .append(0.5 * (double) (hook.getMaxPlotCount() - 1))
				   .append(", fit=")
				   .append(hook.getMinPlotVal())
				   .append(")").toString();
		x1 = scaleX(0.5 * (double) (hook.getMaxPlotCount() - 1))
			 - (3 + g.getFontMetrics().stringWidth(s));
		y1 = scaleY(hook.getMinPlotVal());
		g.drawString(s, x1, y1);

		s = (new StringBuffer()).append("(gen=")
				   .append(0)
				   .append(", fit=")
				   .append(0.5 * (hook.getMaxPlotVal() + hook.getMinPlotVal()))
				   .append(")").toString();
		x1 = scaleX(0);
		y1 = scaleY(0.5 * (hook.getMaxPlotVal() + hook.getMinPlotVal()))
			 - g.getFontMetrics().getHeight() / 3;
		g.drawString(s, x1, y1);

		s = (new StringBuffer()).append("(gen=")
				   .append(hook.getMaxPlotCount() - 1)
				   .append(", fit=")
				   .append(0.5 * (hook.getMaxPlotVal() + hook.getMinPlotVal()))
				   .append(")").toString();
		x1 = scaleX(hook.getMaxPlotCount() - 1) - g.getFontMetrics().stringWidth(s);
		y1 = scaleY(0.5 * (hook.getMaxPlotVal() + hook.getMinPlotVal()))
			 + g.getFontMetrics().getHeight();
		g.drawString(s, x1, y1);

	}

	private void plotFitnessGraph(Graphics g, ArrayList data, Color col) {

		if (null == data || 0 == data.size())
			return;

		g.setColor(col);

		int x2, y2;
		int x1 = getX((AnalysisHook.Entry) data.get(0));
		int y1 = getY((AnalysisHook.Entry) data.get(0));
		for (int i = 1; i < data.size(); i++) {
			x2 = getX((AnalysisHook.Entry) data.get(i));
			y2 = getY((AnalysisHook.Entry) data.get(i));
			g.drawLine(x1, y1, x2, y2);
			x1 = x2;
			y1 = y2;
		}
	}

	private void plotStdDeviationGraph(Graphics g, ArrayList avgFitData, Color col) {

		if (null == avgFitData || 0 == avgFitData.size())
			return;

		g.setColor(col);

		int x2, top_y2, bot_y2;

		AnalysisHook.Entry e = (AnalysisHook.Entry) avgFitData.get(0);

		int top_y1 = scaleY(e.getValue() +
							hook.getStdDeviation(e.getGeneration()).getValue());
		int bot_y1 = scaleY(e.getValue() -
							hook.getStdDeviation(e.getGeneration()).getValue());
		int x1 = scaleX(e.getGeneration());

		for (int i = 1; i < avgFitData.size(); i++) {

			e = (AnalysisHook.Entry) avgFitData.get(i);

			top_y2 = scaleY(e.getValue() +
							hook.getStdDeviation(e.getGeneration()).getValue());
			bot_y2 = scaleY(e.getValue() -
							hook.getStdDeviation(e.getGeneration()).getValue());
			x2 = scaleX(e.getGeneration());

			g.drawLine(x1, top_y1, x2, top_y2);
			g.drawLine(x1, bot_y1, x2, bot_y2);

			top_y1 = top_y2;
			bot_y1 = bot_y2;
			x1 = x2;
		}

	}

	private void plotBestGraph(Graphics g, ArrayList data, Color col) {

		if (null == data || 0 == data.size())
			return;

		g.setColor(col);

		int x2 = getX((AnalysisHook.Entry) data.get(0));
		int y2 = getY((AnalysisHook.Entry) data.get(0));
		int x1, y1;
		for (int i = 1; i < data.size(); i++) {

			x1 = getX((AnalysisHook.Entry) data.get(i-1));
			y1 = getY((AnalysisHook.Entry) data.get(i-1));
			x2 = getX((AnalysisHook.Entry) data.get(i));
			y2 = getY((AnalysisHook.Entry) data.get(i-1));
			g.drawLine(x1, y1, x2, y2);

			x1 = x2;
			y1 = y2;
			x2 = x1;
			y2 = getY((AnalysisHook.Entry) data.get(i));
			g.drawLine(x1, y1, x2, y2);
		}

		x1 = x2;
		y1 = y2;
		x2 = scaleX(hook.getMaxPlotCount() - 1);
		y2 = y1;
		g.drawLine(x1, y1, x2, y2);

		/*
		int x2, y2;
		int x1 = getX((AnalysisHook.Entry) data.get(0));
		int y1 = getY((AnalysisHook.Entry) data.get(0));
		for (int i = 1; i < data.size(); i++) {
			x2 = getX((AnalysisHook.Entry) data.get(i));
			y2 = getY((AnalysisHook.Entry) data.get(i));
			g.drawLine(x1, y1, x2, y2);
			x1 = x2;
			y1 = y2;
		}
		x2 = scaleX(hook.getMaxPlotCount() - 1);
		y2 = y1;
		g.drawLine(x1, y1, x2, y2);
		*/

	}

	private int getX(AnalysisHook.Entry entry) {
		return scaleX(entry.getGeneration());
	}

	private int getY(AnalysisHook.Entry entry) {
		return scaleY(entry.getValue());
	}

	private int scaleX(double x) {
		return (int) (border + x * xFactor);
	}

	private int scaleY(double y) {
		return (int) (getHeight() - border -
					  (y - hook.getMinPlotVal()) * yFactor);
	}

}