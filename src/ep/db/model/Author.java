package ep.db.model;

public class Author {
	
	private long authorId;
	
	private String lastName;
	
	private String middleName;
	
	private String firstName;
	
	private String email;
	
	private String affiliation;

	public Author() {
		
	}
	
	public Author(String lastName, String middleName, String firstName, String email, String affiliation) {
		super();
		this.lastName = lastName;
		this.middleName = middleName;
		this.firstName = firstName;
		this.email = email;
		this.affiliation = affiliation;
	}

	public long getAuthorId() {
		return authorId;
	}

	public void setAuthorId(long authorId) {
		this.authorId = authorId;
	}

	public String getLastName() {
		return lastName;
	}

	public void setLastName(String lastName) {
		this.lastName = lastName;
	}

	public String getMiddleName() {
		return middleName;
	}

	public void setMiddleName(String middleName) {
		this.middleName = middleName;
	}

	public String getFirstName() {
		return firstName;
	}

	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getAffiliation() {
		return affiliation;
	}

	public void setAffiliation(String affiliation) {
		this.affiliation = affiliation;
	}
}
