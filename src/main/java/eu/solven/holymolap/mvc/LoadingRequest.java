package eu.solven.holymolap.mvc;

public class LoadingRequest {
	final String url;

	public LoadingRequest(String url) {
		this.url = url;
	}

	public String getUrl() {
		return url;
	}

}
