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
import be.nabu.libs.types.SimpleTypeWrapperFactory;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.base.ComplexElementImpl;
import be.nabu.libs.types.base.SimpleElementImpl;
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
					Structure extended = null;
					// if we have the filter input, let's also support structured filters
					if (input.get("filter") != null) {
						extended = new Structure();
						extended.setName("input");
						extended.setSuperType(input);
						input = extended;
						extended.add(new ComplexElementImpl("filters", (ComplexType) BeanResolver.getInstance().resolve(Filter.class), extended, 
							new ValueImpl<Integer>(MinOccursProperty.getInstance(), 0),
							new ValueImpl<Integer>(MaxOccursProperty.getInstance(), 0)));
					}
					List<String> pathParameters = client.getPathParameters();
					if (pathParameters != null && !pathParameters.isEmpty()) {
						if (extended == null) {
							extended = new Structure();
							extended.setName("input");
							extended.setSuperType(input);
						}
						input = extended;
						Structure path = new Structure();
						path.setName("path");
						for (String parameter : pathParameters) {
							path.add(new SimpleElementImpl<String>(parameter, SimpleTypeWrapperFactory.getInstance().getWrapper().wrap(String.class), path));
						}
						extended.add(new ComplexElementImpl("path", path, extended));
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
