package ep.db.database;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import cern.colt.matrix.DoubleMatrix2D;
import cern.colt.matrix.impl.SparseDoubleMatrix2D;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedGraph;
import edu.uci.ics.jung.graph.DirectedSparseGraph;
import ep.db.extractor.Utils;
import ep.db.model.Author;
import ep.db.model.Document;

public class DatabaseService {

	private static final String PROP_FILE = "config.properties";

	private static final String GRAPH_SQL = "with nodes as (select row_number() over(order by doc_id ) as row_number, "
			+ "doc_id n from documents) "
			+ "select doc_id as source,row_number as target from citations c "
			+ "inner join nodes ON c.ref_id = nodes.n order by doc_id, row_number";

	private static final String INSERT_DOC = "INSERT INTO documents AS d (title, doi, keywords, abstract, "
			+ "publication_date, volume, pages, issue, container, container_issn, language ) "
			+ "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::regconfig) ON CONFLICT (doi) DO UPDATE "
			+ "SET title = coalesce(d.title, excluded.title),"
			+ "keywords=coalesce(d.keywords, excluded.keywords), "
			+ "abstract = coalesce(d.abstract, excluded.abstract),"
			+ "publication_date = coalesce(d.publication_date, excluded.publication_date), "
			+ "volume = coalesce(d.volume, excluded.volume), "
			+ "pages = coalesce(d.pages, excluded.pages), "
			+ "issue = coalesce(d.issue, excluded.issue), "
			+ "container = coalesce(d.container, excluded.container), "
			+ "container_issn = coalesce(d.container_issn, excluded.container_issn), "
			+ "language = coalesce(d.language, excluded.language) ";

	private static final String INSERT_AUTHOR = "INSERT INTO authors as a (aut_name) "
			+ "VALUES (?) ON CONFLICT (aut_name) DO UPDATE "
			+ "SET aut_name = coalesce(a.aut_name,excluded.aut_name); ";

	private static final String INSERT_DOC_AUTHOR = "INSERT INTO document_authors as a (doc_id,aut_id) VALUES(?,?) "
			+ "ON CONFLICT DO NOTHING";

	private static final String INSERT_REFERENCE = "INSERT INTO citations(doc_id, ref_id) VALUES (?, ?) "
			+ "ON CONFLICT DO NOTHING";

	private static final String DELETE_DOC = "DELETE FROM documents WHERE doc_id = ?";

	private static final String UPDATE_XY = "UPDATE documents_data SET x = ?, y = ? WHERE doc_id = ?";

	private static final String UPDATE_RELEVANCE = "UPDATE documents_data SET relevance = ? WHERE doc_id = ?";

	private Database db;

	private final int batchSize;

	public DatabaseService(Properties config) {
		this.db = new Database(config);
		this.batchSize = Integer.parseInt(config.getProperty("db.batch_size", "100"));
	}

	public int getNumberOfDocuments() throws Exception {
		try ( Connection conn = db.getConnection();){
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT count(*) FROM documents");
			if ( rs.next() ){
				return rs.getInt(1);
			}
			return 0;
		}catch( Exception e){
			throw e;
		}
	}

	public DoubleMatrix2D getGraph() throws Exception{
		try ( Connection conn = db.getConnection();){
			// Recupera numero de citacoes para pre-alocacao
			// da matriz de adjacencia.
			int size = getNumberOfDocuments();
			if ( size == 0)
				return null;

			// Recupera citacoes para construção do 
			// grafo
			PreparedStatement stmt = conn.prepareStatement(GRAPH_SQL);
			try (ResultSet rs = stmt.executeQuery()){
				DoubleMatrix2D graph = new SparseDoubleMatrix2D(size,size);
				graph.assign(0.0);

				int i = 0;
				long lastSource = 0;
				while ( rs.next() ){
					long source = rs.getLong(1);
					int target = rs.getInt(2);
					if ( lastSource != i)
						++i;
					graph.set(i, target-1, 1.0);
					lastSource = source;
				}

				return graph;

			}catch (SQLException e) {
				throw e;
			}

		}catch( Exception e){
			throw e;
		}
	}

	public DoubleMatrix2D buildFrequencyMatrix(long[] docIds) throws Exception {
		// Retorna numero de documentos e ocorrencia total dos termos
		int numberOfDocuments;
		if ( docIds == null )
			numberOfDocuments = getNumberOfDocuments();
		else
			numberOfDocuments = docIds.length;

		// Constroi consulta caso docIds != null
		StringBuilder sql = new StringBuilder();
		if ( docIds != null ){
			sql.append(" WHERE doc_id IN (");
			sql.append(docIds[0]);
			for(int i = 1; i < docIds.length; i++){
				sql.append(",");
				sql.append(docIds[i]);
			}
			sql.append(")");
		}

		String where = sql.toString();
		final Map<String, Integer> termsFreq = getTermsFrequency(where);
		final Map<String, Integer> termsToColumnMap = new HashMap<>();

		// Mapeamento termo -> coluna na matriz (bag of words)
		int c = 0;
		for(String key : termsFreq.keySet()){
			termsToColumnMap.put(key, c);
			++c;
		}

		DoubleMatrix2D matrix = new SparseDoubleMatrix2D(numberOfDocuments, termsFreq.size());

		// Popula matriz com frequencia dos termos em cada documento
		buildFrequencyMatrix(matrix, termsFreq, termsToColumnMap, where, true );

		return matrix;
	}

	public long addDocument(Document doc) throws Exception {
		long docId = -1;
		try ( Connection conn = db.getConnection();){
			PreparedStatement stmt = conn.prepareStatement(INSERT_DOC, Statement.RETURN_GENERATED_KEYS);
			stmt.setString(1, doc.getTitle());
			stmt.setString(2, doc.getDOI());
			stmt.setString(3, doc.getKeywords());
			stmt.setString(4, doc.getAbstract());
			stmt.setInt(5, Utils.extractYear(doc.getPublicationDate()));
			stmt.setString(6, doc.getVolume());
			stmt.setString(7, doc.getPages());
			stmt.setString(8, doc.getIssue());
			stmt.setString(9, doc.getContainer());
			stmt.setString(10, doc.getISSN());
			stmt.setString(11, doc.getLanguage());
			stmt.executeUpdate();
			ResultSet rs = stmt.getGeneratedKeys();
			if (rs.next()){
				docId = rs.getLong(1);
				doc.setDocId(docId);
			}
		}catch( Exception e){
			throw e;
		}

		if ( docId > 0 ){
			addAuthors(Arrays.asList(doc));
			addDocumetAuthors(Arrays.asList(doc));
		}

		return docId;
	}

	public long[] addDocuments(List<Document> documents) throws Exception {
		List<Long> docIds = new ArrayList<>();

		try ( Connection conn = db.getConnection();){
			PreparedStatement stmt = conn.prepareStatement(INSERT_DOC, Statement.RETURN_GENERATED_KEYS);

			int count = 0;

			for( Document doc: documents ){
				stmt.setString(1, doc.getTitle());
				stmt.setString(2, doc.getDOI());
				stmt.setString(3, doc.getKeywords());
				stmt.setString(4, doc.getAbstract());
				stmt.setInt(5, Utils.extractYear(doc.getPublicationDate()));
				stmt.setString(6, doc.getVolume());
				stmt.setString(7, doc.getPages());
				stmt.setString(8, doc.getIssue());
				stmt.setString(9, doc.getContainer());
				stmt.setString(10, doc.getISSN());
				stmt.setString(11, doc.getLanguage());
				stmt.addBatch();

				if (++count % batchSize == 0){
					stmt.executeBatch();
					getGeneratedKeys(docIds, stmt.getGeneratedKeys());
				}
			}

			stmt.executeBatch();
			getGeneratedKeys(docIds, stmt.getGeneratedKeys());

			for(int i = 0; i < docIds.size(); i++)
				documents.get(i).setDocId(docIds.get(i));

		}catch( Exception e){
			throw e;
		}

		if ( docIds.size() > 0 ){
			addAuthors(documents);
			addDocumetAuthors(documents);
		}

		return docIds.stream().mapToLong(l->l).toArray();
	}

	private long[] addAuthors(List<Document> documents) throws Exception {
		try ( Connection conn = db.getConnection();){
			PreparedStatement stmt = conn.prepareStatement(INSERT_AUTHOR, Statement.RETURN_GENERATED_KEYS);
			int count = 0;

			List<Long> ids = new ArrayList<>();

			for( Document doc : documents){
				for( Author author : doc.getAuthors() ){
					stmt.setString(1, author.getName());
					stmt.addBatch();

					if(++count % batchSize == 0){
						stmt.executeBatch();
						getGeneratedKeys(ids, stmt.getGeneratedKeys());
					}
				}
			}

			stmt.executeBatch();
			getGeneratedKeys(ids, stmt.getGeneratedKeys());

			int i = 0;
			for( Document doc : documents){
				for( Author author : doc.getAuthors() ){
					author.setAuthorId(ids.get(i));
					++i;
				}
			}

			return ids.stream().mapToLong(l->l).toArray();

		}catch( Exception e){
			throw e;
		}
	}

	private void addDocumetAuthors(List<Document> docs) throws Exception {
		try ( Connection conn = db.getConnection();){
			PreparedStatement stmt = conn.prepareStatement(INSERT_DOC_AUTHOR, Statement.RETURN_GENERATED_KEYS);

			int count = 0;
			for( Document doc : docs ){
				for( Author aut : doc.getAuthors() ){
					stmt.setLong(1, doc.getDocId());
					stmt.setLong(2,aut.getAuthorId());
					stmt.addBatch();

					if (++count % batchSize == 0){
						stmt.executeBatch();
					}
				}
			}
			stmt.executeBatch();
		}catch (Exception e) {
			throw e;
		}
	}

	private void getGeneratedKeys(List<Long> ids, ResultSet generatedKeys) throws SQLException {
		while (generatedKeys.next()){
			ids.add(generatedKeys.getLong(1));
		}
	}

	//TODO: change argument to int type
	public void deleteDocument(long id) throws Exception {
		try ( Connection conn = db.getConnection();){
			PreparedStatement stmt = conn.prepareStatement(DELETE_DOC);
			stmt.setLong(1, id);
			stmt.executeUpdate();
		}catch( Exception e){
			throw e;
		}
	}

	public void addReference(long docId, Document ref) throws Exception {
		long refId = addDocument(ref);
		if ( refId > 0){
			try ( Connection conn = db.getConnection();){
				PreparedStatement stmt = conn.prepareStatement(INSERT_REFERENCE);
				stmt.setLong(1, docId);
				stmt.setLong(2, refId);
				stmt.executeUpdate();
			}catch( Exception e){
				throw e;
			}
		}
	}

	public void addReferences(long docId, List<Document> refs) throws Exception {
		long[] refIds = addDocuments(refs);
		try ( Connection conn = db.getConnection();){
			PreparedStatement stmt = conn.prepareStatement(INSERT_REFERENCE);
			for(int i = 0; i < refIds.length; i++){
				if ( refIds[i] > 0){
					stmt.setLong(1, docId);
					stmt.setLong(2, refIds[i]);
					stmt.addBatch();
				}
			}
			stmt.executeBatch();
		}catch( Exception e){
			throw e;
		}
	}

	private TreeMap<String, Integer> getTermsFrequency(String where) throws Exception {
		try ( Connection conn = db.getConnection();){

			String sql = "SELECT word,nentry FROM ts_stat('SELECT tsv FROM documents";
			if ( where != null && !where.isEmpty() )
				sql += where;
			sql += "') WHERE nentry > 1 AND ndoc > 1";

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			TreeMap<String,Integer> termsFreq = new TreeMap<>();
			while( rs.next() ){
				String term = rs.getString("word");
				int freq = rs.getInt("nentry");
				termsFreq.put(term, freq);
			}
			return termsFreq;

		}catch( Exception e){
			throw e;
		}
	}

	public void buildFrequencyMatrix(DoubleMatrix2D matrix, Map<String, Integer> termsFreq,
			Map<String, Integer> termsToColumnMap, String where, boolean normalize) throws Exception {
		try ( Connection conn = db.getConnection();){

			String sql = "SELECT freqs FROM documents";
			if ( where != null)
				sql += where;
			sql += " ORDER BY doc_id";

			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			int doc = 0;
			while( rs.next() ){
				String terms = rs.getString("freqs");
				if ( terms != null && !terms.isEmpty() ){
					ObjectMapper mapper = new ObjectMapper();
					List<Map<String,Object>> t = mapper.readValue(terms, 
							new TypeReference<List<Map<String,Object>>>(){});
					for(Map<String,Object> o : t){
						String term = (String) o.get("word");
						if ( termsToColumnMap.containsKey(term)){
							double freq = ((Integer) o.get("nentry")).doubleValue();
							if ( normalize )
								freq /= termsFreq.get(term);
							int col = termsToColumnMap.get(term);
							matrix.setQuick(doc, col, freq);
						}
					}
				}
				++doc;
			}
		}catch( Exception e){
			throw e;
		}
	}

	public void updateXYProjections(DoubleMatrix2D y) throws Exception {
		Connection conn = null;
		try { 
			conn = db.getConnection();
			conn.setAutoCommit(false);

			PreparedStatement pstmt = conn.prepareStatement(UPDATE_XY);
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT doc_id FROM documents ORDER BY doc_id");
			int doc = 0;
			while( rs.next() ){
				long id = rs.getLong("doc_id");
				pstmt.setDouble(1, y.get(doc, 0));
				pstmt.setDouble(2, y.get(doc, 1));
				pstmt.setLong(3, id);
				pstmt.addBatch();
				++doc;

				if ( doc % 50 == 0)
					pstmt.executeBatch();
			}

			pstmt.executeBatch();
			conn.commit();

		}catch( Exception e){
			if ( conn != null )
				conn.rollback();
			throw e;
		}finally {
			if ( conn != null )
				conn.close();
		}
	}

	public DirectedGraph<Long,Long> getCitationGraph() throws Exception {

		//		final Map<Long, Integer> docIndexMap = getDocumentsIndexMapping();
		//		int n = docIndexMap.size();

		try ( Connection conn = db.getConnection();){
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery("SELECT doc_id, ref_id FROM citations ORDER BY doc_id, ref_id");

			DirectedGraph<Long, Long> graph = new DirectedSparseGraph<>();
			long e = 0;
			while( rs.next() ){
				long docId = rs.getLong(1);
				long refId = rs.getLong(2);

				//				int source = docIndexMap.get(docId);
				//				int target = docIndexMap.get(refId);

				if ( !graph.containsVertex(docId))
					graph.addVertex(docId);
				if ( !graph.containsVertex(refId))
					graph.addVertex(refId);

				graph.addEdge(e, docId, refId);
				++e;
			}
			return graph;

		}catch( Exception e){
			throw e;
		}
	}

	public void updatePageRank(DirectedGraph<Long, Long> graph, PageRank<Long,Long> pageRank) throws Exception {
		Connection conn = null;
		try { 
			conn = db.getConnection(); 
			conn.setAutoCommit(false);

			PreparedStatement pstmt = conn.prepareStatement(UPDATE_RELEVANCE);
			int i = 0;
			for(Long docId : graph.getVertices()){
				pstmt.setDouble(1, pageRank.getVertexScore(docId));
				pstmt.setLong(2, docId);
				pstmt.addBatch();
				++i;

				if (i % 50 == 0)
					pstmt.executeBatch();
			}

			pstmt.executeBatch();
			conn.commit();

		}catch( Exception e){
			if ( conn != null )
				conn.rollback();
			throw e;
		}finally {
			if ( conn != null )
				conn.close();
		}

	}

	public static void main(String[] args) throws Exception {

		Properties properties = new Properties();
		properties.load(new FileInputStream(PROP_FILE));

		Database db = new Database(properties);
		db.getConnection();

		DatabaseService ss = new DatabaseService(properties);

		//		DoubleMatrix2D graph = ss.getGraph();
		//		for(int i = 0; i < graph.rows(); i++){
		//			for(int j = 0; j < graph.columns(); j++)
		//				System.out.print(String.format("%d ", (int) graph.getQuick(i, j)));
		//			System.out.println();
		//		}

		int ndocs = ss.getNumberOfDocuments();

		String where = " WHERE doc_id IN (1,2,3)";
		TreeMap<String, Integer> termsFreq = ss.getTermsFrequency(where);
		final Map<String, Integer> termsToColumnMap = new HashMap<>();
		int c = 0;
		for(String key : termsFreq.keySet()){
			termsToColumnMap.put(key, c);
			++c;
		}
		DoubleMatrix2D matrix = new SparseDoubleMatrix2D(ndocs, termsFreq.size());
		ss.buildFrequencyMatrix(matrix, termsFreq, termsToColumnMap, where, true);
		System.out.println(matrix.toStringShort());
	}
}
