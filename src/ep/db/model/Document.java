package ep.db.model;

import java.util.List;

import edu.uci.ics.jung.algorithms.scoring.PageRank;

/**
 * Classe representando um documento armanzeado
 * no banco de dados
 * @version 1.0
 * @since 2017
 *
 */
public class Document {

	/**
	 * Id do documento no banco de dados
	 */
	private long docId;
	
	/**
	 * DOI
	 */
	private String doi;

	/**
	 * Título
	 */
	private String title;

	/**
	 * Palavras-chaves
	 */
	private String keywords;

	/**
	 * Lista de autores
	 */
	private List<Author> authors;

	/**
	 * Resumo
	 */
	private String _abstract;

	/**
	 * Data de publicação
	 */
	private String publicationDate;
	
	/**
	 * Volume
	 */
	private String volume;
	
	/**
	 * Página(s)
	 */
	private String pages;
	
	/**
	 * Issue
	 */
	private String issue;
	
	/**
	 * Nome do journal
	 */
	private String container;
	
	/**
	 * ISSN do jounal
	 */
	private String ISSN;
	
	/**
	 * Língua do documento.
	 */
	private String language;
	
	/**
	 * Coordenada x da projeção
	 */
	private double x;
	
	/**
	 * Coordenada y da projeção
	 */
	private double y;
	
	/**
	 * Relevância pelo {@link PageRank}
	 */
	private double relevance;
	
	/**
	 * Score relativo a consulta (PostgreSQL rank)
	 */
	private double rank;

	private String url;

	/**
	 * Cria um novo documento
	 */
	public Document() {

	}
	
	/**
	 * Retorna id do documento.
	 * @return id do documento.
	 */
	public long getDocId() {
		return docId;
	}

	/**
	 * Atribui id do documento.
	 * @param docId id do documento.
	 */
	public void setDocId(long docId) {
		this.docId = docId;
	}

	/**
	 * Retorna DOI do documento.
	 * @return DOI.
	 */
	public String getDOI() {
		return doi;
	}

	/**
	 * Atribui DOI.
	 * @param doi DOI
	 */
	public void setDOI(String doi) {
		this.doi = doi;
	}

	/**
	 * Retorna título do documento.
	 * @return título.
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * Atribui título do documento.
	 * @param title título.
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * Retorna palavras-chaves.
	 * @return palavras-chaves.
	 */
	public String getKeywords() {
		return keywords;
	}

	/**
	 * Atribui palavras-chaves.
	 * @param keywords palavras-chaves.
	 */
	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	/**
	 * Retorna autores do documento.
	 * @return lista de autores.
	 */
	public List<Author> getAuthors() {
		return authors;
	}

	/** 
	 * Atribui autores do documento.
	 * @param authors lista de autores.
	 */
	public void setAuthors(List<Author> authors) {
		this.authors = authors;
	}

	/**
	 * Retorna resumo.
	 * @return resumo.
	 */
	public String getAbstract() {
		return _abstract;
	}

	/**
	 * Atribui resumo.
	 * @param _abstract resumo.
	 */
	public void setAbstract(String _abstract) {
		this._abstract = _abstract;
	}

	/**
	 * Retorna data de publicação do documento.
	 * @return data de publicação (geralmente somente
	 * ano).
	 */
	public String getPublicationDate() {
		return publicationDate;
	}

	/**
	 * Atribui data de publicação.
	 * @param publicationDate data de publicação.
	 */
	public void setPublicationDate(String publicationDate) {
		this.publicationDate = publicationDate;
	}

	/**
	 * Retorna volume do material de divulgação (journal).
	 * @return volume.
	 */
	public String getVolume() {
		return volume;
	}

	/**
	 * Atribui volume do material de divulgação (journal).
	 * @param volume volume.
	 */
	public void setVolume(String volume) {
		this.volume = volume;
	}

	/**
	 * Retorna página(s) onde o documento foi
	 * divulgado. 
	 * @return página(s).
	 */
	public String getPages() {
		return pages;
	}

	/**
	 * Atribui página(s) onde o documento foi
	 * divulgado.
	 * @param pages página(s).
	 */
	public void setPages(String pages) {
		this.pages = pages;
	}

	/**
	 * Retorna issue.
	 * @return issue.
	 */
	public String getIssue() {
		return issue;
	}

	/**
	 * Atribui issue.
	 * @param issue issue.
	 */
	public void setIssue(String issue) {
		this.issue = issue;
	}

	/**
	 * Retorna nome do material de divulgação 
	 * (e.g nome do journal).
	 * @return nome do journal.
	 */
	public String getContainer() {
		return container;
	}

	/**
	 * Atribui nome do material de divulgação 
	 * (e.g nome do journal).
	 * @param container nome do journal.
	 */
	public void setContainer(String container) {
		this.container = container;
	}

	/**
	 * Retorna ISSN do journal.
	 * @return ISSN
	 */
	public String getISSN() {
		return ISSN;
	}

	/**
	 * Atribui ISSN do journal.
	 * @param iSSN ISSN.
	 */
	public void setISSN(String iSSN) {
		ISSN = iSSN;
	}

	/**
	 * Retorna língua do documento.
	 * @return código da línga em ISO-3166.
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * Atribui língua do documento.
	 * @param language código da língua.
	 */
	public void setLanguage(String language) {
		this.language = language;
	}

	/**
	 * Retorna coordenada x da projeção.
	 * @return coordenada x.
	 */
	public double getX() {
		return x;
	}

	/**
	 * Atribui coordenada x da projeção.
	 * @param x coordenda x.
	 */
	public void setX(double x) {
		this.x = x;
	}

	/**
	 * Retorna coordenada y da projeção.
	 * @return coordenada y.
	 */
	public double getY() {
		return y;
	}

	/**
	 * Atribui coordenada y da projeção.
	 * @param y coordenada y
	 */
	public void setY(double y) {
		this.y = y;
	}

	/**
	 * Retorna relavância do documento
	 * calculada a partir do algoritmo {@link PageRank}.
	 * @return relevância do documento em relação a toda a base 
	 * (no. de citações).
	 */
	public double getRelevance() {
		return relevance;
	}

	/**
	 * Atribui relevância do documento.
	 * @param relevance relevância.
	 */
	public void setRelevance(double relevance) {
		this.relevance = relevance;
	}

	/**
	 * Retorna rank do documento segundo consulta
	 * feita no banco de dados (PostgreSQL rank).
	 * @return rank do documento.
	 */
	public double getRank() {
		return rank;
	}

	/**
	 * Atribui rank do documento.
	 * @param rank rank (PostgreSQL).
	 */
	public void setRank(double rank) {
		this.rank = rank;
	}
	
	/**
	 * Retorna URL para o documento
	 * @return URL do documento
	 */
	public String getUrl() {
		return url;
	}
	
	/**
	 * Atribu o URL para o documento.
	 * @param url URL externo para o documento.
	 */
	public void setURL(String url) {
		this.url = url;
	}

	/**
	 * Retorna uma representação textual do documento.
	 */
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		if ( getAuthors() != null)
			sb.append(getAuthors() + " ");
		if ( getTitle() != null )
			sb.append(getTitle() + " ");
		if (getPublicationDate() != null)
			sb.append(getPublicationDate() + " ");
		return sb.toString().trim(); 
	}

	
}
