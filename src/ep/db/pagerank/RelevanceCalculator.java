package ep.db.pagerank;

import java.io.FileInputStream;
import java.util.Properties;

import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedGraph;
import ep.db.database.DatabaseService;

public class RelevanceCalculator {
	
	private static final String PROP_FILE = "config.properties";
	
	private static final double C = 0.85;
	
	private final DatabaseService dbService;
	
	private final double c;
	
	public RelevanceCalculator( Properties config ) {
		this(config, C);
	}
	
	public RelevanceCalculator( Properties config, double c ) {
		this.dbService = new DatabaseService(config);
		this.c = c;
	}
	
	public void updateRelevance() throws Exception {
		
		DirectedGraph<Long,Long> graph = null;
		try {
			graph = dbService.getCitationGraph();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
		
		PageRank<Long, Long> pageRank = new PageRank<>(graph, c);
		pageRank.evaluate(); 
		
		try {
			dbService.updatePageRank(graph, pageRank);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
	}
	
	public static void main(String[] args) {
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(PROP_FILE));
			
			RelevanceCalculator ranking = new RelevanceCalculator(properties);
			ranking.updateRelevance();

		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}

}
