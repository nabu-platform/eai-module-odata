/*
* Copyright (C) 2022 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.eai.module.odata.client;

import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import be.nabu.eai.module.odata.client.api.ODataRequestRewriter;
import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.artifacts.jaxb.JAXBArtifact;
import be.nabu.eai.repository.util.SystemPrincipal;
import be.nabu.libs.http.HTTPException;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.core.HTTPRequestAuthenticatorFactory;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.parser.ODataParser;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.pojo.POJOUtils;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;
import nabu.protocols.http.client.Services;

public class ODataClient extends JAXBArtifact<ODataClientConfiguration> {

	private ODataDefinition definition;
	
	public ODataClient(String id, ResourceContainer<?> directory, Repository repository) {
		super(id, directory, repository, "odata-client.xml", ODataClientConfiguration.class);
	}

	private ODataRequestRewriter rewriter;
	private boolean rewriterResolved;
	
	public ODataRequestRewriter getRewriter() {
		if (!rewriterResolved) {
			synchronized(this) {
				if (!rewriterResolved) {
					DefinedService requestRewriter = getConfig().getRequestRewriter();
					if (requestRewriter != null) {
						this.rewriter = POJOUtils.newProxy(ODataRequestRewriter.class, requestRewriter, getRepository(), SystemPrincipal.ROOT);
					}
					rewriterResolved = true;
				}
			}
		}
		return rewriter;
	}
	
	public List<String> getPathParameters() {
		List<String> parameters = new ArrayList<String>();
		String path = getConfig().getEndpoint().getPath();
		if (path != null) {
			Pattern pattern = Pattern.compile("\\{[^}]+\\}");
			java.util.regex.Matcher matcher = pattern.matcher(path);
			while (matcher.find()) {
				String match = matcher.group();
				match = match.substring(1, match.length() - 1).trim();
				parameters.add(match);
			}
		}
		return parameters;
	}
	
	public ODataDefinition getDefinition() {
		try {
			if (definition == null && getConfig().getEndpoint() != null) {
				synchronized(this) {
					if (definition == null && getConfig().getEndpoint() != null) {
						Resource child = getDirectory().getChild("odata-metadata.xml");
						ODataParser parser = getParser();
						// forward any expansion requirements
						parser.setEntityConfigurations(getConfig().getExpansions());
						// in development, we will backfeed the definition
						if (child == null && EAIResourceRepository.isDevelopment()) {
							InputStream metadata = parser.getMetadata(getConfig().getEndpoint());
							if (metadata != null) {
								try {
									child = ((ManageableContainer<?>) getDirectory()).create("odata-metadata.xml", "application/xml");
									WritableContainer<ByteBuffer> writable = ((WritableResource) child).getWritable();
									try {
										IOUtils.copyBytes(IOUtils.wrap(metadata), writable);
									}
									finally {
										writable.close();
									}
								}
								finally {
									metadata.close();
								}
							}
						}
						if (child != null) {
							try (ReadableContainer<ByteBuffer> readable = ((ReadableResource) child).getReadable()) {
								definition = parser.parse(getConfig().getEndpoint(), IOUtils.toInputStream(readable));
							}
						}
					}
				}
			}
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
		return definition;
	}

	public ODataParser getParser() {
		try {
			ODataParser parser = new ODataParser();
			if (getConfig().getHttpClient() != null) {
				parser.setHttpClient(Services.newClient(getConfig().getHttpClient()));
			}
			parser.setBaseId(getId());
			return parser;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	public void refreshMetadata() {
		
	}
	
	public InputStream getMetadata(ODataClient client, URI url, boolean authenticate) {
		URI child = URIUtils.getChild(url, "$metadata");
		HTTPClient httpClient = client.getParser().getHTTPClient();
		HTTPRequest request = new DefaultHTTPRequest("GET", child.getPath(), new PlainMimeEmptyPart(null, 
			new MimeHeader("Content-Length", "0"),
			new MimeHeader("Accept", "application/xml"),
			new MimeHeader("User-Agent", "User agent"),
			new MimeHeader("Host", child.getHost())
		));
		// TODO: we could use the security here as well...?
		// but this is client side, maybe we need to push this to server side?
		try {
			HTTPResponse response = httpClient.execute(request, null, child.getScheme().equals("https"), true);
			if (authenticate && client.getConfig().getSecurityType() != null) {
				Map<String, Object> original = ServiceRuntime.getGlobalContext();
				HashMap<String, Object> runtimeContext = new HashMap<String, Object>();
				runtimeContext.put("service.context", client.getId());
				ServiceRuntime.setGlobalContext(runtimeContext);
				try {
					if (!HTTPRequestAuthenticatorFactory.getInstance().getAuthenticator(client.getConfig().getSecurityType())
							.authenticate(request, client.getConfig().getSecurityContext(), null, false)) {
						throw new IllegalStateException("Could not authenticate the request");
					}
				}
				finally {
					ServiceRuntime.setGlobalContext(original);
				}
			}
			// if we have a 401 and we are currently doing an unauthenticated call, check if we can make an authenticated one
			if (response.getCode() == 401 && !authenticate && client.getConfig().getSecurityType() != null) {
				return getMetadata(client, url, true);
			}
			else if (response.getCode() >= 200 && response.getCode() < 300) {
				if (response.getContent() instanceof ContentPart) {
					ReadableContainer<ByteBuffer> readable = ((ContentPart) response.getContent()).getReadable();
					return IOUtils.toInputStream(readable);
				}
				else {
					throw new IllegalStateException("The response does not contain any content");
				}
			}
			else {
				throw new HTTPException(response.getCode());
			}
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
