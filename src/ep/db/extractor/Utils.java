package ep.db.extractor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.text.WordUtils;
import org.grobid.core.data.Person;

import ep.db.model.Author;

public final class Utils {


	public static final String AUTHOR_SEPARATOR = ";";

	private static final Pattern YEAR_PATTERN = Pattern.compile("\\w*(\\d{4})\\w*");


	public static String normalizeAuthors(List<String> authors){
		if (authors.isEmpty())
			return null;
		StringBuilder sb = new StringBuilder();
		for(String author : authors){
			sb.append(sanitize(author));
			sb.append(AUTHOR_SEPARATOR);
		}
		sb.replace(sb.length()-1, sb.length(), "");
		return sb.toString();
	}

	public static String sanitize(String text){
		if (text != null){
			String ret = text.replaceAll("\\[|,|;|\\.|\\-|\\]", " ").replaceAll("\\s{2}", " ").trim().toLowerCase();
			return WordUtils.capitalize(ret);
		}
		return "";
	}

	public static String normalizePerson(List<Person> authors) {

		if (authors == null)
			return null;

		StringBuilder sb = new StringBuilder();
		for(Person p : authors){
			sb.append(Utils.sanitize(p.getLastName()));
			sb.append(" ");
			sb.append(Utils.sanitize(p.getFirstName()));
			sb.append(" ");
			sb.append(Utils.sanitize(p.getMiddleName()));
			sb.append(Utils.AUTHOR_SEPARATOR);
		}
		sb.replace(sb.length()-1, sb.length(), "");
		return sb.toString().replaceAll("\\s+;", ";").replaceAll("\\s{2,}", "");
	}

	public static String languageToISO3166(String language) {
		if ( language != null ){
			switch (language){
			//languages are encoded in ISO 3166
			case "en":
				return "english";
			default:
				return "english";
			}
		}
		return "english";
	}

	public static int extractYear(String publicationDate) {
		if (publicationDate == null)
			return 0;
		String year;
		Matcher m = YEAR_PATTERN.matcher(publicationDate);
		if (m.matches())
			year = m.group(1);
		else 
			year = publicationDate;

		try{
			return Integer.parseInt(year);
		}catch (NumberFormatException e) {}
		return 0;
	}

	public static List<Author> getAuthors(String authors) {
		if ( authors != null ){
			String[] arr = authors.split(";");
			List<Author> list = new ArrayList<>(arr.length);
			for(String a : arr){
				Author author = new Author(Utils.sanitize(a));
				list.add(author);
			}	
			return list;
		}
		return new ArrayList<>(0);
	}
}
