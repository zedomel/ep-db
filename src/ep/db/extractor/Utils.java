package ep.db.extractor;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.text.WordUtils;

import ep.db.model.Author;

/**
 * Classe com método auxiliares. 
 * @version 1.0
 * @since 2017
 *
 */
public final class Utils {

	/**
	 * Padrão de expressão regular para extração de ano a partir de datas.
	 */
	private static final Pattern YEAR_PATTERN = Pattern.compile("\\w*(\\d{4})\\w*");
	
	/**
	 * Separador de nomes de autores em String.
	 */
	private static final String AUTHORS_SEPARATOR = ";";

	/**
	 * Remove caracteres especiais de Strings.
	 * @param text texto a ser processado.
	 * @return text sem caracteres especiais.
	 */
	public static String sanitize(String text){
		if (text != null){
			String ret = text.replaceAll("\\[|,|;|\\.|\\-|\\]", " ").replaceAll("\\s{2}", " ").trim().toLowerCase();
			return WordUtils.capitalize(ret);
		}
		return "";
	}
	
	public static String languageToISO3166(String language) {
		if ( language != null ){
			switch (language){
			//languages are encoded in ISO-3166
			case "en":
				return "english";
			default:
				return "english";
			}
		}
		return "english";
	}

	/**
	 * Extrai ano a partir de datas
	 * @param date data completa ou não.
	 * @return ano contido na data informamado 
	 * como um inteiro.
	 */
	public static int extractYear(String date) {
		if (date == null)
			return 0;
		String year;
		Matcher m = YEAR_PATTERN.matcher(date);
		if (m.matches())
			year = m.group(1);
		else 
			year = date;

		try{
			return Integer.parseInt(year);
		}catch (NumberFormatException e) {}
		return 0;
	}

	/**
	 * Retorna lista de autores a partir de string 
	 * contendo um ou mais nomes de autores.
	 * @param authors String contendo nomes de autores
	 * separados por {@value #AUTHORS_SEPARATOR}.
	 * @return lista de autores.
	 */
	public static List<Author> getAuthors(String authors) {
		if ( authors != null ){
			String[] arr = authors.split(AUTHORS_SEPARATOR);
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
