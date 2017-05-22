package ep.db.mdp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.util.Properties;

import cern.colt.matrix.DoubleMatrix2D;
import ep.db.database.DatabaseService;

public class MultidimensionalProjection {

	private static final String PROP_FILE = "config.properties";
	
	private DatabaseService dbService;

	public MultidimensionalProjection( Properties config ) {
		this.dbService = new DatabaseService(config);
	}

	public void project() {

		// Constroi matriz de frequencia de termos
		DoubleMatrix2D matrix = null;
		try {
			 matrix = dbService.buildFrequencyMatrix(null);
		} catch (Exception e) {
			// TODO Log-me
			e.printStackTrace();
		}

		File csv = new File("bag_of_words_small.csv");
		try (BufferedWriter bw = new BufferedWriter(new FileWriter(csv))){
			
			for(int i = 0; i < matrix.rows(); i++){
				bw.write(String.format("%f", matrix.getQuick(i, 0)));
				for(int j = 1; j < matrix.columns(); j++)
					bw.write(String.format(",%f", matrix.getQuick(i, j)));
				bw.newLine();
			}
			
		}catch (Exception e) {
			// TODO: handle exception
		}
		// Realiza projeção multidimensional
		Lamp lamp = new Lamp();
		DoubleMatrix2D y = lamp.project(matrix);
		
		updateProjections(y);
	}
	
	private void updateProjections(DoubleMatrix2D y) {
		try {
			dbService.updateXYProjections(y);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
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
