package be.nabu.eai.module.odata.client;

import java.io.IOException;
import java.util.List;

import be.nabu.eai.developer.MainController;
import be.nabu.eai.developer.managers.base.BaseJAXBGUIManager;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.libs.property.api.Property;
import be.nabu.libs.property.api.Value;

public class ODataClientGUIManager extends BaseJAXBGUIManager<ODataClientConfiguration, ODataClient> {

	public ODataClientGUIManager() {
		super("OData Client", ODataClient.class, new ODataClientManager(), ODataClientConfiguration.class);
	}
	
	@Override
	public String getCategory() {
		return "Services";
	}

	protected List<Property<?>> getCreateProperties() {
		return null;
	}

	@Override
	protected ODataClient newInstance(MainController controller, RepositoryEntry entry, Value<?>...values) throws IOException {
		return new ODataClient(entry.getId(), entry.getContainer(), entry.getRepository());
	}

}
