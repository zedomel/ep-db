package ep.db.mdp;

import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import cern.colt.matrix.DoubleMatrix2D;
import ep.db.database.DatabaseService;

/**
 * Classe para realizar projeção multidimensional
 * utilizando {@link Lamp}.
 * @version 1.0
 * @since 2017
 *
 */
public class MultidimensionalProjection {

	/**
	 * Arquivo de configuração
	 */
	private static final String PROP_FILE = "config.properties";
	
	/**
	 * Logger
	 */
	private static Logger logger = Logger.getLogger(MultidimensionalProjection.class);
	
	/**
	 * Serviço para manipulação do banco de dados.
	 */
	private DatabaseService dbService;

	/**
	 * Cria novo objeto para projeção multidimensional
	 * com a configuração dada.
	 * @param config configuração.
	 */
	public MultidimensionalProjection( Properties config ) {
		this.dbService = new DatabaseService(config);
	}

	/**
	 * Realiza projeção multidimensional dos documentos
	 * no banco de dados.
	 * @throws Exception erro ao realizar projeção.
	 */
	public void project() throws Exception {

		// Constroi matriz de frequência de termos
		DoubleMatrix2D matrix = null;
		try {
			 matrix = dbService.buildFrequencyMatrix(null);
		} catch (Exception e) {
			logger.error("Error building frequency matrix", e);
			throw e;
		}

		// Realiza projeção multidimensional utilizando LAMP
		Lamp lamp = new Lamp();
		DoubleMatrix2D y = lamp.project(matrix);
		
		// Atualiza projeções no banco de dados.
		updateProjections(y);
	}
	
	/**
	 * Atualiza projeções no banco de dados.
	 * @param y
	 * @throws Exception 
	 */
	private void updateProjections(DoubleMatrix2D y) throws Exception {
		try {
			dbService.updateXYProjections(y);
		} catch (Exception e) {
			logger.error("Error updating projections in database", e);
			throw e;
		}
	}
	
	/**
	 * Método main para calcúlo das projeções.
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			Properties properties = new Properties();
			properties.load(new FileInputStream(PROP_FILE));
			
			System.out.println("Updating MDP...");
			MultidimensionalProjection mdp = new MultidimensionalProjection(properties);
			mdp.project();
			System.out.println("MDP successful updated");

		} catch (Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
