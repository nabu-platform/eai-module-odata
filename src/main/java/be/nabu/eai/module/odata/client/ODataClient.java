package be.nabu.eai.module.odata.client;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import be.nabu.eai.repository.EAIResourceRepository;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.artifacts.jaxb.JAXBArtifact;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.parser.ODataParser;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import nabu.protocols.http.client.Services;

public class ODataClient extends JAXBArtifact<ODataClientConfiguration> {

	private ODataDefinition definition;
	
	public ODataClient(String id, ResourceContainer<?> directory, Repository repository) {
		super(id, directory, repository, "odata-client.xml", ODataClientConfiguration.class);
	}

	public ODataDefinition getDefinition() {
		try {
			if (definition == null && getConfig().getEndpoint() != null) {
				synchronized(this) {
					if (definition == null && getConfig().getEndpoint() != null) {
						Resource child = getDirectory().getChild("odata-metadata.xml");
						ODataParser parser = getParser();
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
}
