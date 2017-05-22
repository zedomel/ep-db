package ep.db.extractor;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.grobid.core.data.BibDataSet;
import org.grobid.core.data.BiblioItem;
import org.grobid.core.engines.Engine;
import org.grobid.core.factory.GrobidFactory;
import org.grobid.core.mock.MockContext;
import org.grobid.core.utilities.GrobidProperties;

import ep.db.model.Author;
import ep.db.model.Document;

/**
 * GROBID document parser.
 * Classe para processar documentos (artigos cientificos)
 * utilizando GROBID.
 * @author jose
 *
 */
public final class GrobIDDocumentParser implements DocumentParser{

	private static Engine ENGINE;

	private Engine engine;

	private BiblioItem metadata;

	private List<BibDataSet> references;

	private final boolean consolidate;

	private final String grobidHome;

	private final String grobidProperties;

	public GrobIDDocumentParser(String grobidHome, String grobidProperties) throws Exception {
		this(grobidHome, grobidProperties, false);
	}

	/**
	 * Cria um novo {@link GrobIDDocumentParser} (singleton)
	 * @param consolidate: the consolidation option allows GROBID to 
	 * exploit Crossref web services for improving header information
	 * @throws Exception se uma exceção ocorrer ao carregar GROBID
	 */
	public GrobIDDocumentParser(String grobidHome, String grobidProperties, boolean consolidate) throws Exception {
		this.consolidate = consolidate;
		this.grobidHome = grobidHome;
		this.grobidProperties = grobidProperties;
		initialize();
		engine = ENGINE;
	}

	/**
	 * Inicializa GROBID
	 * @throws Exception
	 */
	private void initialize() throws Exception {			
		try {
			MockContext.setInitialContext(grobidHome, grobidProperties);
		} catch (Exception e1) {
			throw e1;
		}
		GrobidProperties.getInstance();	
		ENGINE = GrobidFactory.getInstance().createEngine();
	}

	/**
	 * Returna a engine do GROBID
	 * @return grobid engine
	 */
	public Engine getEngine() {
		return engine;
	}

	@Override
	public void parseHeader(String filename) {
		metadata = new BiblioItem();
		getEngine().processHeader(filename, consolidate , metadata);
	}

	@Override
	public void parseReferences(String filename) {
		references = getEngine().processReferences(new File(filename), consolidate);
	}

	@Override
	public List<Author> getAuthors() {
		if (metadata.getAuthors() != null)
			return Utils.getAuthors(metadata.getAuthors());
		return null;
	}

	@Override
	public String getLanguage() {
		return Utils.languageToISO3166(metadata.getLanguage());
	}

	@Override
	public String getTitle() {
		return metadata.getTitle();
	}

	@Override
	public String getAffiliation() {
		return metadata.getAffiliation();
	}

	@Override
	public String getDOI() {
		return metadata.getDOI();
	}

	@Override
	public String getPublicationDate() {
		return metadata.getPublicationDate() == null ? metadata.getYear() : 
			metadata.getPublicationDate();
	}

	@Override
	public String getAbstract() {
		return metadata.getAbstract();
	}

	@Override
	public String getJournal() {
		return metadata.getJournal();
	}

	@Override
	public String getKeywords() {
		if (metadata.getKeywords() != null )
			return metadata.getKeywords().stream().map(s -> s.getKeyword().toString()).collect(Collectors.joining(", "));
		return null;
	}

	@Override
	public String getContainer() {
		String container = null;
		if ( metadata.getJournal() != null )
			container = metadata.getJournal();
		else if (metadata.getBookTitle() != null)
			container = metadata.getBookTitle();
		else if ( metadata.getEvent() != null )
			container = metadata.getEvent();
		return container;
	}

	@Override
	public String getIssue() {
		return metadata.getIssue();
	}

	@Override
	public String getISSN() {
		return metadata.getISSN() != null ? metadata.getISSN() : metadata.getISSNe();
	}

	@Override
	public String getPages() {
		return metadata.getPageRange() != null ? metadata.getPageRange() : (
				metadata.getBeginPage() + "-" + metadata.getEndPage());
	}

	@Override
	public String getVolume() {
		return metadata.getVolume();
	}

	@Override
	public List<Document> getReferences() {
		List<Document> refs = new ArrayList<>(references.size());
		for(BibDataSet bds : references){
			BiblioItem bib = bds.getResBib();
			if (!metadata.getDOI().equals(bib.getDOI())){
				Document ref = new Document();

				ref.setDOI(bib.getDOI());
				ref.setAuthors(Utils.getAuthors(bib.getAuthors()));
				ref.setTitle(bib.getTitle());
				ref.setAbstract(bib.getAbstract());
				ref.setIssue(bib.getIssue());
				if (bib.getKeywords() != null && !bib.getKeywords().isEmpty() )
					ref.setKeywords(bib.getKeywords().stream().map(s -> s.getKeyword().toString()).collect(Collectors.joining(", ")));
				ref.setLanguage(Utils.languageToISO3166(bib.getLanguage()));
				ref.setPages(bib.getPageRange());
				ref.setVolume(bib.getVolume());

				// Set Container ISSN
				if ( bib.getISSN() != null )
					ref.setISSN(bib.getISSN());
				else if (bib.getISSNe() != null )
					ref.setISSN(bib.getISSNe());

				// Set Container Name
				if ( bib.getJournal() != null )
					ref.setContainer(bib.getJournal());
				else if ( bib.getEvent() != null )
					ref.setContainer(bib.getEvent());
				else if ( bib.getBookTitle() != null )
					ref.setContainer(bib.getBookTitle());

				//Set publication date
				if ( bib.getPublicationDate() != null )
					ref.setPublicationDate( bib.getPublicationDate() );
				else if ( bib.getYear() != null ) 
					ref.setPublicationDate( bib.getYear() );

				refs.add(ref);
			}
		}
		return refs;
	}
}
