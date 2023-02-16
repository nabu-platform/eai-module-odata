package be.nabu.eai.module.odata.client;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import be.nabu.eai.repository.util.Filter;
import be.nabu.libs.artifacts.ExternalDependencyImpl;
import be.nabu.libs.artifacts.api.ExternalDependency;
import be.nabu.libs.artifacts.api.ExternalDependencyArtifact;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.types.Function;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.services.api.ExecutionContext;
import be.nabu.libs.services.api.Service;
import be.nabu.libs.services.api.ServiceException;
import be.nabu.libs.services.api.ServiceInstance;
import be.nabu.libs.services.api.ServiceInterface;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.base.ComplexElementImpl;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.MinOccursProperty;
import be.nabu.libs.types.structure.Structure;

public class ODataClientService implements DefinedService, ExternalDependencyArtifact {

	private Function function;
	private String id;
	private ODataClient client;
	private ComplexType input;

	public ODataClientService(String id, ODataClient client, Function function) {
		this.id = id;
		this.client = client;
		this.function = function;
	}
	
	@Override
	public ServiceInterface getServiceInterface() {
		return new ServiceInterface() {
			@Override
			public ServiceInterface getParent() {
				return null;
			}
			@Override
			public ComplexType getOutputDefinition() {
				return function.getOutput();
			}
			
			@Override
			public ComplexType getInputDefinition() {
				if (ODataClientService.this.input == null) {
					ComplexType input = function.getInput();
					// if we have the filter input, let's also support structured filters
					if (input.get("filter") != null) {
						Structure extended = new Structure();
						extended.setName("input");
						extended.setSuperType(input);
						extended.add(new ComplexElementImpl("filters", (ComplexType) BeanResolver.getInstance().resolve(Filter.class), extended, 
							new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
						input = extended;
					}
					ODataClientService.this.input = input;
				}
				return ODataClientService.this.input;
			}
		};
	}

	@Override
	public ServiceInstance newInstance() {
		return new ServiceInstance() {
			@Override
			public Service getDefinition() {
				return ODataClientService.this;
			}
			@Override
			public ComplexContent execute(ExecutionContext executionContext, ComplexContent input) throws ServiceException {
				return new ODataRunner(client).run(function, input);
			}
		};
	}

	@Override
	public Set<String> getReferences() {
		return new HashSet<String>();
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public List<ExternalDependency> getExternalDependencies() {
		List<ExternalDependency> dependencies = new ArrayList<ExternalDependency>();
		ExternalDependencyImpl dependency = new ExternalDependencyImpl();
		ODataDefinition definition = client.getDefinition();
		try {
			dependency.setEndpoint(new URI(
				definition.getScheme(),
				definition.getHost(),
				definition.getBasePath(),
				null,
				null));
		}
		catch (URISyntaxException e) {
			// can't help it...
		}
		dependency.setArtifactId(getId());
		dependency.setMethod(function.getMethod());
		dependency.setGroup(client.getId());
		dependency.setType("REST");
		dependencies.add(dependency);
		return dependencies;
	}

}
