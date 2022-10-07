package be.nabu.eai.module.odata.client;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import be.nabu.eai.api.Advanced;
import be.nabu.eai.module.http.client.HTTPClientArtifact;
import be.nabu.eai.repository.jaxb.ArtifactXMLAdapter;
import be.nabu.libs.types.api.annotation.Field;

@XmlRootElement(name = "odata")
public class ODataClientConfiguration {
	private HTTPClientArtifact httpClient;
	private Charset charset;
	private URI endpoint;
	// the operation ids to expose!
	private List<String> operationIds = new ArrayList<String>();
	
	@Field(comment = "You can opt for using a specific http client, for example if you are working with self-signed certificates for internal infrastructure. If left empty, the default http client will be used.")
	@Advanced
	@XmlJavaTypeAdapter(value = ArtifactXMLAdapter.class)
	public HTTPClientArtifact getHttpClient() {
		return httpClient;
	}
	public void setHttpClient(HTTPClientArtifact httpClient) {
		this.httpClient = httpClient;
	}
	public Charset getCharset() {
		return charset;
	}
	public void setCharset(Charset charset) {
		this.charset = charset;
	}
	public URI getEndpoint() {
		return endpoint;
	}
	public void setEndpoint(URI endpoint) {
		this.endpoint = endpoint;
	}
	public List<String> getOperationIds() {
		return operationIds;
	}
	public void setOperationIds(List<String> operationIds) {
		this.operationIds = operationIds;
	}
}
