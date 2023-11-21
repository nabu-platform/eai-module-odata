package be.nabu.eai.module.odata.client;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import be.nabu.eai.api.Advanced;
import be.nabu.eai.api.InterfaceFilter;
import be.nabu.eai.api.ValueEnumerator;
import be.nabu.eai.developer.impl.HTTPAuthenticatorEnumerator;
import be.nabu.eai.module.http.client.HTTPClientArtifact;
import be.nabu.eai.repository.jaxb.ArtifactXMLAdapter;
import be.nabu.libs.odata.parser.ODataExpansion;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.types.api.annotation.Field;

@XmlRootElement(name = "odata")
public class ODataClientConfiguration {
	private HTTPClientArtifact httpClient;
	private Charset charset;
	private URI endpoint;
	// the entitySets to expose!
	private List<String> entitySets = new ArrayList<String>();
	private List<ODataExpansion> expansions = new ArrayList<ODataExpansion>();
	
	// the type of the security needed (depends on whats available)
	private String securityType;
	// the security context within that type
	private String securityContext;
	
	private DefinedService requestRewriter;
	
	// http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_KeyasSegmentConvention
	// some providers (e.g. sharepoint) expect segments. others (e.g. business central) do not
	private boolean keyAsSegment;
	private boolean ignoreEtag;
	
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
	public List<String> getEntitySets() {
		return entitySets;
	}
	public void setEntitySets(List<String> entitySets) {
		this.entitySets = entitySets;
	}
	@ValueEnumerator(enumerator = HTTPAuthenticatorEnumerator.class)
	public String getSecurityType() {
		return securityType;
	}
	public void setSecurityType(String securityType) {
		this.securityType = securityType;
	}
	public String getSecurityContext() {
		return securityContext;
	}
	public void setSecurityContext(String securityContext) {
		this.securityContext = securityContext;
	}
	
	@Advanced
	@XmlJavaTypeAdapter(value = ArtifactXMLAdapter.class)
	@InterfaceFilter(implement = "be.nabu.eai.module.odata.client.api.ODataRequestRewriter.rewrite")
	public DefinedService getRequestRewriter() {
		return requestRewriter;
	}
	public void setRequestRewriter(DefinedService requestRewriter) {
		this.requestRewriter = requestRewriter;
	}
	
	public List<ODataExpansion> getExpansions() {
		return expansions;
	}
	public void setExpansions(List<ODataExpansion> expansions) {
		this.expansions = expansions;
	}
	
	@Advanced
	@Field(comment = "By default we will send a If-Match: * header but some systems (like ms graph) don't like this. They do allow sending no header at all. If you enable this, we will not send a header unless it is explicitly known.")
	public boolean isIgnoreEtag() {
		return ignoreEtag;
	}
	public void setIgnoreEtag(boolean ignoreEtag) {
		this.ignoreEtag = ignoreEtag;
	}
	
	@Advanced
	public boolean isKeyAsSegment() {
		return keyAsSegment;
	}
	public void setKeyAsSegment(boolean keyAsSegment) {
		this.keyAsSegment = keyAsSegment;
	}
}
