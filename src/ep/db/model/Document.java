package ep.db.model;

import java.util.List;

/**
 * Classe representando um documento armanzeado
 * no banco de dados
 * @author jose
 *
 */
public class Document {

	private long docId;
	
	private String doi;

	private String title;

	private String keywords;

	private List<Author> authors;

	private String _abstract;

	private String publicationDate;
	
	private String volume;
	
	private String pages;
	
	private String issue;
	
	private String container;
	
	private String ISSN;
	
	private String language;
	
	private double x;
	
	private double y;
	
	private double relevance;
	
	private double rank;

	/**
	 * Cria um novo documento
	 */
	public Document() {

	}
	
	public long getDocId() {
		return docId;
	}

	public void setDocId(long docId) {
		this.docId = docId;
	}

	public String getDOI() {
		return doi;
	}

	public void setDOI(String doi) {
		this.doi = doi;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getKeywords() {
		return keywords;
	}

	public void setKeywords(String keywords) {
		this.keywords = keywords;
	}

	public List<Author> getAuthors() {
		return authors;
	}

	public void setAuthors(List<Author> authors) {
		this.authors = authors;
	}

	public String getAbstract() {
		return _abstract;
	}

	public void setAbstract(String _abstract) {
		this._abstract = _abstract;
	}

	public String getPublicationDate() {
		return publicationDate;
	}

	public void setPublicationDate(String publicationDate) {
		this.publicationDate = publicationDate;
	}

	public String getVolume() {
		return volume;
	}

	public void setVolume(String volume) {
		this.volume = volume;
	}

	public String getPages() {
		return pages;
	}

	public void setPages(String pages) {
		this.pages = pages;
	}

	public String getIssue() {
		return issue;
	}

	public void setIssue(String issue) {
		this.issue = issue;
	}

	public String getContainer() {
		return container;
	}

	public void setContainer(String container) {
		this.container = container;
	}

	public String getISSN() {
		return ISSN;
	}

	public void setISSN(String iSSN) {
		ISSN = iSSN;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	public double getX() {
		return x;
	}

	public void setX(double x) {
		this.x = x;
	}

	public double getY() {
		return y;
	}

	public void setY(double y) {
		this.y = y;
	}

	public double getRelevance() {
		return relevance;
	}

	public void setRelevance(double relevance) {
		this.relevance = relevance;
	}

	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

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
