package ep.db.model;

public class Author {
	
	private long authorId;
	
	private String name;

	public Author() {
		
	}
	
	public Author(String name) {
		super();
		this.name = name;
	}

	public long getAuthorId() {
		return authorId;
	}

	public void setAuthorId(long authorId) {
		this.authorId = authorId;
	}

	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
}
