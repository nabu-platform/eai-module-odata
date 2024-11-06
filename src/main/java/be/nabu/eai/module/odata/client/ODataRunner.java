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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlElement;

import be.nabu.eai.module.odata.client.api.ODataRequestRewriter;
import be.nabu.eai.repository.util.Filter;
import be.nabu.libs.converter.ConverterFactory;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.core.HTTPRequestAuthenticatorFactory;
import be.nabu.libs.http.core.HTTPUtils;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.parser.ODataEntityConfiguration;
import be.nabu.libs.odata.types.Function;
import be.nabu.libs.odata.types.NavigationProperty;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.Marshallable;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.api.annotation.Field;
import be.nabu.libs.types.binding.api.MarshallableBinding;
import be.nabu.libs.types.binding.api.UnmarshallableBinding;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.java.BeanResolver;
import be.nabu.libs.types.mask.MaskedContent;
import be.nabu.libs.types.properties.AliasProperty;
import be.nabu.libs.types.properties.CollectionNameProperty;
import be.nabu.libs.types.properties.DuplicateProperty;
import be.nabu.libs.types.properties.ForeignKeyProperty;
import be.nabu.libs.types.properties.ForeignNameProperty;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.api.Header;
import be.nabu.utils.mime.api.ModifiablePart;
import be.nabu.utils.mime.impl.FormatException;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.MimeUtils;
import be.nabu.utils.mime.impl.PlainMimeContentPart;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;
import nabu.protocols.http.client.Services;

/**
 * To update bindings, we currently do this:
 * 
 * the update function injects both the local fields (start with _ in dynamics for instance)
 * it will inject a complex type to represent the binding (in case you want to create a new one)
 * it will inject a string with the name of @odata.bind to use an existing binding
 * 
 */
public class ODataRunner {
	private ODataDefinition definition;
	private ODataClient client;

	public ODataRunner(ODataClient client) {
		this.client = client;
		this.definition = client == null ? null : client.getDefinition();
	}
	
	private HTTPResponse run(String transactionId, HTTPRequest...requests) throws KeyStoreException, NoSuchAlgorithmException, IOException, FormatException, ParseException {
		HTTPResponse response = null;
		for (HTTPRequest request : requests) {
			if (client.getConfig().getSecurityType() != null) {
				if (!HTTPRequestAuthenticatorFactory.getInstance().getAuthenticator(client.getConfig().getSecurityType())
					.authenticate(request, client.getConfig().getSecurityContext(), null, false)) {
					throw new IllegalStateException("Could not authenticate the request");
				}
			}
			
			ODataRequestRewriter rewriter = client.getRewriter();
			if (rewriter != null) {
				rewriter.rewrite(client.getId(), request);
			}
				
			HTTPClient client = Services.getTransactionable(ServiceRuntime.getRuntime().getExecutionContext(), transactionId == null ? null : transactionId.toString(), this.client.getConfig().getHttpClient()).getClient();
			response = client.execute(request, null, "https".equals(definition.getScheme()), true);
			HTTPUtils.validateResponse(response);
		}
		return response;
	}
	
	public static class Association {
		private String odataId;

		@Field(alias = "@odata.id")
		public String getOdataId() {
			return odataId;
		}

		public void setOdataId(String odataId) {
			this.odataId = odataId;
		}
	}
	
	public static class AssociationList {
		private List<Association> associations;

		@Field(name = "value")
		public List<Association> getAssociations() {
			return associations;
		}

		public void setAssociations(List<Association> associations) {
			this.associations = associations;
		}
	}
	
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ComplexContent run(Function function, ComplexContent input) {
		try {
			Object transactionId = input == null ? null : input.get("transactionId");
			
			String target = definition.getBasePath();
			
			Element<?> pathElement = input.getType().get("path");
			// if we have a path element, we likely have variables in the path, check it and replace it
			if (pathElement != null) {
				for (Element<?> child : TypeUtils.getAllChildren((ComplexType) pathElement.getType())) {
					String value = (String) input.get("path/" + child.getName());
					// don't replace if you don't fill it in, it might be part of the url?
					if (value != null) {
						target = target.replaceAll("\\{[\\s]*" + child.getName() + "[\\s]*\\}", value == null ? "" : value);
					}
				}
			}
			
			Collection<Element<?>> inputChildren = TypeUtils.getAllChildren(function.getInput());
			Collection<Element<?>> outputChildren = TypeUtils.getAllChildren(function.getOutput());
			
			ComplexType usedType = null;
			// check for parent ids for contained navigation properties
			for (Element<?> element : inputChildren) {
				// we have an input?
				if (element.getType() instanceof ComplexType) {
					usedType = (ComplexType) element.getType();
					ComplexContent functionInput = input == null ? null : (ComplexContent) input.get(element.getName());
					if (functionInput != null) {
						for (Element<?> child : TypeUtils.getAllChildren(functionInput.getType())) {
							// if it is a parent, add it to the path
							int indexOf = child.getName().indexOf("@odata.parent.id");
							if (indexOf > 0) {
								Object parentValue = functionInput.get(child.getName());
								if (parentValue != null) {
									if (!(parentValue instanceof Iterable)) {
										parentValue = Arrays.asList(parentValue);
									}
									String entitySetName = child.getName().substring(0, indexOf);
									for (Object singleParentValue : (Iterable) parentValue) {
										if (singleParentValue != null) {
											if (client.getConfig().isKeyAsSegment()) {
												// keys can be given as segments
												// in sharepoint the segment way works /sites/id but the default /sites(id) does not
												// http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_KeyasSegmentConvention
												target += "/" + entitySetName + "/" + ((Marshallable) child.getType()).marshal(singleParentValue, child.getProperties());
											}
											else {
												target += "/" + entitySetName + "(" + ((Marshallable) child.getType()).marshal(singleParentValue, child.getProperties()) + ")";
											}
										}
									}
								}
							}
						}
					}
				}
			}
			// we want to merge the associations
			// according to the documentation, any contained relations are added:
			// http://docs.oasis-open.org/odata/odata/v4.0/errata02/os/complete/part1-protocol/odata-v4.0-errata02-os-part1-protocol-complete.html#_Toc406398329
			// "The entity MUST NOT contain related entities as inline content. It MAY contain binding information for navigation properties. For single-valued navigation properties this replaces the relationship. For collection-valued navigation properties this adds to the relationship.
			// TODO: we could do this in one transaction using $batch: https://learn.microsoft.com/en-us/dynamics365/business-central/dev-itpro/webservices/use-odata-batch
			Charset charset = client.getConfig().getCharset();
			if (charset == null) {
				charset = Charset.forName("UTF-8");
			}
			// TODO: probably does not work for pure "containstarget", the absolute ids used for creating new associations do not take this into account
			if ("MERGE-ASSOCIATIONS".equals(function.getMethod()) || "ADD-ASSOCIATIONS".equals(function.getMethod()) || "REMOVE-ASSOCIATIONS".equals(function.getMethod())) {
				// typeEntity == function.getContext() -> the entitysetname
				// navigation property -> boundIds alias
				// entityId -> input 
				// boundEntityId -> input
				// boundEntityType -> boundIds collectionName
				
				Element<?> boundIdsElement = function.getInput().get("boundIds");
				Element<?> entityIdElement = function.getInput().get("entityId");
				
				String boundEntityCollection = ValueUtils.getValue(CollectionNameProperty.getInstance(), boundIdsElement.getProperties());
				String navigationProperty = ValueUtils.getValue(AliasProperty.getInstance(), boundIdsElement.getProperties());
				
				String entityId = ((Marshallable) entityIdElement.getType()).marshal(input.get("entityId"), entityIdElement.getProperties());
				List boundIds = (List) input.get("boundIds");
				// convert to string
				if (boundIds != null) {
					for (int i = 0; i < boundIds.size(); i++) {
						boundIds.set(i, ((Marshallable) boundIdsElement.getType()).marshal(boundIds.get(i), boundIdsElement.getProperties()));
					}
				}
				else {
					boundIds = new ArrayList();
				}

				// all calls are against the type name
				target += "/" + function.getContext();
				if (client.getConfig().isKeyAsSegment()) {
					target += "/" + entityId;
				}
				else {
					target += "(" + entityId + ")";
				}
				target += "/" + navigationProperty; 
				
				// key is the id itself, the value is the "full" path to delete it
				Map<String, String> existingAssociations = new HashMap<String, String>();
				
				// first we list the existing associations so we know which ones to delete and add
				// e.g. GET /api/data/v9.2/{typeEntity}({entityId})/{navigationProperty}/$ref
				// returns a list of "value" objects which each contain a single string field: odataId
				String listTarget = target;
				listTarget += "/$ref";
				
				if ("MERGE-ASSOCIATIONS".equals(function.getMethod())) {
					ModifiablePart part = new PlainMimeEmptyPart(null, 
						new MimeHeader("Content-Length", "0"),
						new MimeHeader("Accept", "application/json"),
						new MimeHeader("Host", definition.getHost())
					);
					DefaultHTTPRequest request = new DefaultHTTPRequest("GET", listTarget, part);
					HTTPResponse response = run((String) transactionId, request);
					JSONBinding binding = new JSONBinding((ComplexType) BeanResolver.getInstance().resolve(AssociationList.class), charset);
					binding.setIgnoreUnknownElements(true);
					ReadableContainer<ByteBuffer> readable = ((ContentPart) response.getContent()).getReadable();
					if (readable != null) {
						try {
							AssociationList list = TypeUtils.getAsBean(binding.unmarshal(IOUtils.toInputStream(readable), new Window[0]), AssociationList.class);
							if (list.getAssociations() != null) {
								for (Association association : list.getAssociations()) {
									String odataId = association.getOdataId();
									if (odataId != null) {
										String id = odataId.replaceAll(".*\\(([^)]+)\\).*?", "$1");
										existingAssociations.put(id, odataId);
									}
								}
							}
						}
						finally {
							readable.close();
						}
					}
				}
				else if ("REMOVE-ASSOCIATIONS".equals(function.getMethod())) {
					// we report them as existing
					for (Object boundId : boundIds) {
						String odataBaseUrl = definition.getScheme() + "://" + definition.getHost() + definition.getBasePath();
	//					String odataContext = odataBaseUrl + "/$metadata#Collection($ref)";
						//String body = "{\"@odata.context\": \"" + odataContext + "\", \"@odata.id\": \"" + boundEntityCollection + "(" + boundId + ")" + "\"}";
						String entityUrl = odataBaseUrl + "/" + boundEntityCollection + (client.getConfig().isKeyAsSegment() ? "/" + boundId : "(" + boundId + ")");
						existingAssociations.put(boundId.toString(), entityUrl);
					}
					// we want to clear the boundids so they are marked for deletion
					boundIds = new ArrayList();
				}
				
				List<DefaultHTTPRequest> requests = new ArrayList<DefaultHTTPRequest>();
				
				// it appears you can not do a batch delete of all references: https://stackoverflow.com/questions/24213664/delete-all-related-odata-entities-in-one-request-from-client
				// then delete all the ones that are no longer necessary
				// e.g. DELETE /api/data/v9.2/{typeEntity}({entityId})/{navigationProperty}({boundEntityId})/$ref
				for (String key : existingAssociations.keySet()) {
					// no longer exists, add a delete for it
					if (boundIds.indexOf(key) < 0) {
						// not sure if this needs to be key-as-segmented?
						ModifiablePart part = new PlainMimeEmptyPart(null, 
							new MimeHeader("Content-Length", "0"),
							new MimeHeader("Accept", "application/json"),
							new MimeHeader("Host", definition.getHost())
						);
						// the url is absolute
//						request = new DefaultHTTPRequest("DELETE", target + "(" + existingAssociations.get(key) + ")/$ref", part);
						String deleteTarget = client.getConfig().isKeyAsSegment() ? target + "/" + key + "/$ref" : target + "(" + key + ")/$ref";
						DefaultHTTPRequest request = new DefaultHTTPRequest("DELETE", deleteTarget, part);
						requests.add(request);
					}
				}
				
				// append all the new ones
				// e.g. PUT /api/data/v9.2/{typeEntity}({entityId})/{navigationProperty}/$ref
				// body: {"odataId": "..."}
				// must be a relative uri /{boundEntityType}({boundEntityId})
				for (String boundId : (List<String>) boundIds) {
					if (!existingAssociations.containsKey(boundId)) {
						// not sure if this needs to be key-as-segmented?
						String odataBaseUrl = definition.getScheme() + "://" + definition.getHost() + definition.getBasePath();
//						String odataContext = odataBaseUrl + "/$metadata#Collection($ref)";
						//String body = "{\"@odata.context\": \"" + odataContext + "\", \"@odata.id\": \"" + boundEntityCollection + "(" + boundId + ")" + "\"}";
						String body = "{\"@odata.id\": \"" + odataBaseUrl + "/" + boundEntityCollection + (client.getConfig().isKeyAsSegment() ? "/" + boundId : "(" + boundId + ")") + "\"}";
						byte[] bytes = body.getBytes(charset);
						ModifiablePart part = new PlainMimeContentPart(null, IOUtils.wrap(bytes, true),
							new MimeHeader("Content-Length", Integer.toString(bytes.length)),
							new MimeHeader("Content-Type", "application/json"),
							new MimeHeader("Accept", "application/json"),
							new MimeHeader("Host", definition.getHost())
						);
						((PlainMimeContentPart) part).setReopenable(true);
						DefaultHTTPRequest request = new DefaultHTTPRequest(client.getConfig().isUsePostForRelations() ? "POST" : "PUT", target + "/$ref", part);
						requests.add(request);
					}
				}
				if (requests.size() > 0) {
					run((String) transactionId, requests.toArray(new HTTPRequest[requests.size()]));
				}
				return function.getOutput().newInstance();
			}
			else {
				// if we do not have a complex type in the input, check the output
				if (usedType == null) {
					for (Element<?> element : outputChildren) {
						// we have an input?
						if (element.getType() instanceof ComplexType) {
							usedType = (ComplexType) element.getType();
						}
					}
				}
				
				// if we have filters, check if you are filtering on the parent ids, we also need to add them then!
				List<Filter> filters = input == null ? null : (List<Filter>) input.get("filters");
				if (filters != null && !filters.isEmpty()) {
					// don't modify the original list
					filters = new ArrayList<Filter>(filters);
					Iterator<Filter> iterator = filters.iterator();
					while (iterator.hasNext()) {
						Object filterObject = iterator.next();
						if (filterObject instanceof MaskedContent) {
							filterObject = ((MaskedContent) filterObject).getOriginal();
						}
						if (filterObject instanceof BeanInstance) {
							filterObject = ((BeanInstance<?>) filterObject).getUnwrapped();
						}
						Filter filter = (Filter) filterObject;
						int indexOf = filter.getKey().indexOf("@odata.parent.id");
						if (indexOf > 0) {
							if (filter.getValues() != null && !filter.getValues().isEmpty()) {
								String entitySetName = filter.getKey().substring(0, indexOf);
								for (Object singleParentValue : filter.getValues()) {
									if (singleParentValue != null) {
										// keys can be given as segments
										// in sharepoint the segment way works /sites/id but the default /sites(id) does not so we use this as default for now
										// in the future we may want to offer configuration to tweak this behavior because it is hard to rewrite this into the other syntax
										// http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_KeyasSegmentConvention
										Element<?> filterElement = usedType == null ? null : usedType.get(filter.getKey());
										String stringified = null;
										if (filterElement == null) {
											stringified = ConverterFactory.getInstance().getConverter().convert(singleParentValue, String.class);
										}
										else {
											stringified = ((Marshallable) filterElement.getType()).marshal(singleParentValue, filterElement.getProperties());
										}
										if (client.getConfig().isKeyAsSegment()) {
											// keys can be given as segments
											// in sharepoint the segment way works /sites/id but the default /sites(id) does not
											// http://docs.oasis-open.org/odata/odata/v4.01/odata-v4.01-part2-url-conventions.html#sec_KeyasSegmentConvention
											target += "/" + entitySetName + "/" + stringified;
										}
										else {
											target += "/" + entitySetName + "(" + stringified + ")";
											
										}
									}
								}	
							}
							iterator.remove();
						}
					}
				}
				
				// the context is set to the entity set name
				target += "/" + function.getContext();
				ComplexContent functionInput = null;
				for (Element<?> element : inputChildren) {
					// if we have a primary key field, we are likely doing a specific get or an update/delete
					// either way we have to pass it in the URL
					Boolean primaryKey = ValueUtils.getValue(PrimaryKeyProperty.getInstance(), element.getProperties());
					if (primaryKey != null && primaryKey) {
						target += "(";
						boolean closeQuotes = false;
						// uuids don't need quotes, nor do numbers
						if (String.class.isAssignableFrom(((SimpleType<?>) element.getType()).getInstanceClass())) {
							target += "'";
							closeQuotes = true;
						}
						target += ((Marshallable) element.getType()).marshal(input.get(element.getName()), element.getProperties());
						if (closeQuotes) {
							target += "'";
						}
						target += ")";
					}
					// we have an input?
					if (element.getType() instanceof ComplexType) {
						functionInput = (ComplexContent) input.get(element.getName());
					}
				}
				
				Integer limit = input == null ? null : (Integer) input.get("limit");
				Long offset = input == null ? null : (Long) input.get("offset");
				Boolean totalCount = input == null ? null : (Boolean) input.get("totalCount");
				String search = input == null ? null : (String) input.get("search");
				String filter = input == null ? null : (String) input.get("filter");
				List<String> orderBy = input == null ? null : (List<String>) input.get("orderBy");
				
				boolean queryBegun = false;
				if (limit != null) {
					if (queryBegun) {
						target += "&";
					}
					else {
						queryBegun = true;
						target += "?";
					}
					target += "$top=" + limit;
				}
				if (offset != null) {
					if (queryBegun) {
						target += "&";
					}
					else {
						queryBegun = true;
						target += "?";
					}
					target += "$skip=" + offset;
				}
				if (totalCount != null) {
					if (queryBegun) {
						target += "&";
					}
					else {
						queryBegun = true;
						target += "?";
					}
					target += "$count=" + totalCount;
				}
				if (search != null) {
					if (queryBegun) {
						target += "&";
					}
					else {
						queryBegun = true;
						target += "?";
					}
					target += "$search=" + URIUtils.encodeURL(search);
				}
				if (orderBy != null && !orderBy.isEmpty()) {
					if (queryBegun) {
						target += "&";
					}
					else {
						queryBegun = true;
						target += "?";
					}
					target += "$orderby=";
					boolean first = true;
					for (String single : orderBy) {
						if (first) {
							first = false;
						}
						else {
							target += ",";
						}
						target += URIUtils.encodeURL(single);
					}
				}
				// if you didn't set an explicit filter, you might have used the filters array
				if (filter == null && filters != null && !filters.isEmpty()) {
					filter = buildFilter(filters);
				}
				if (filter != null && !filter.trim().isEmpty()) {
					if (queryBegun) {
						target += "&";
					}
					else {
						queryBegun = true;
						target += "?";
					}
					target += "$filter=" + URIUtils.encodeURL(filter);
				}
					
				// if we are getting, we need to keep track of expansion
				// we use the duplicate property for that
				if ("GET".equalsIgnoreCase(function.getMethod())) {
					String expand = null;
					for (Element<?> child : outputChildren) {
						String value = ValueUtils.getValue(DuplicateProperty.getInstance(), child.getProperties());
						if (value != null && !value.trim().isEmpty()) {
							if (expand == null) {
								expand = value;
							}
							else {
								expand += "," + value;
							}
						}
					}
					if (expand != null) {
						if (queryBegun) {
							target += "&";
						}
						else {
							queryBegun = true;
							target += "?";
						}
						target += "$expand=" + URIUtils.encodeURL(expand);
					}
				}
				
				ModifiablePart part = null;
				byte [] content = null;
				if (functionInput != null) {
					MarshallableBinding binding = null;
					// currently only json
					String contentType = "application/json";
					
					if ("application/json".equals(contentType)) {
						binding = new JSONBinding(functionInput.getType(), charset);
						// especially because we are using a PATCH method, we don't want to force this!
						((JSONBinding) binding).setMarshalNonExistingRequiredFields(false);
						// when we are using the odata.bind stuff, we need to be able to set raw values
						((JSONBinding) binding).setAllowRaw(true);
					}
	
					// update the foreign keys
					if ("PUT".equalsIgnoreCase(function.getMethod()) || "PATCH".equalsIgnoreCase(function.getMethod()) || "POST".equalsIgnoreCase(function.getMethod())) {
						scanForForeignKeys(functionInput);
					}
					
					ByteArrayOutputStream output = new ByteArrayOutputStream();
					binding.marshal(output, functionInput);
					content = output.toByteArray();
					part = new PlainMimeContentPart(null, IOUtils.wrap(content, true),
						new MimeHeader("Content-Length", Integer.toString(content.length)),
						new MimeHeader("Content-Type", contentType)
					);
					((PlainMimeContentPart) part).setReopenable(true);
				}
				else {
					part = new PlainMimeEmptyPart(null, 
						new MimeHeader("Content-Length", "0")
					);
				}
				part.setHeader(new MimeHeader("Accept", "application/json"));
				part.setHeader(new MimeHeader("Host", definition.getHost()));
				
				// in theory we could use the odata etag we get back from the GET
				// but in reality, we don't care (at this point)
				// maybe in the future we'll annotate the instances etc, but for now we leave it like this
				// the star is a special syntax indicating that we don't really care what the current version is, we just want to update it
				if ("PUT".equalsIgnoreCase(function.getMethod()) || "DELETE".equalsIgnoreCase(function.getMethod()) || "PATCH".equalsIgnoreCase(function.getMethod())) {
					if (!client.getConfig().isIgnoreEtag()) {
						part.setHeader(new MimeHeader("If-Match", "*"));
					}
				}
				
				DefaultHTTPRequest request = new DefaultHTTPRequest(function.getMethod(), target, part);
				
				HTTPResponse response = run((String) transactionId, request);
				HTTPUtils.validateResponse(response);
				// we did a create and want to check for a header that indicates the id of
				if (response.getCode() == 204 && "POST".equalsIgnoreCase(function.getMethod())) {
					Header header = MimeUtils.getHeader("OData-EntityId", response.getContent().getHeaders());
					if (header == null) {
						header = MimeUtils.getHeader("Location", response.getContent().getHeaders());
					}
					if (header != null) {
						// check if there is a field to put it in
						Collection<Element<?>> allChildren = outputChildren;
						Iterator<Element<?>> iterator = allChildren.iterator();
						if (iterator.hasNext()) {
							Element<?> field = iterator.next();
							// this is actually the full URI to the item, we just want to extract the id
							String fullHeaderValue = MimeUtils.getFullHeaderValue(header);
							// for example: https://bebat-dev.crm4.dynamics.com/api/data/v9.2/nrq_registrations(358b0d2a-f3a6-ed11-aad1-6045bd957895)
							String id = fullHeaderValue.replaceAll("^http.*/[^/]+\\(([^)]+)\\)$", "$1");
							ComplexContent newInstance = function.getOutput().newInstance();
							newInstance.set(field.getName(), id);
							return newInstance;
						}
					}
				}
				else if (response.getContent() instanceof ContentPart) {
					ReadableContainer<ByteBuffer> readable = ((ContentPart) response.getContent()).getReadable();
					if (readable != null) {
						try {
							UnmarshallableBinding unmarshallable = null;
	
							boolean isListBinding = false;
							String resultName = null;
							ComplexType result = null;
							for (Element<?> element : outputChildren) {
								if (element.getType() instanceof ComplexType) {
									Integer maxOccurs = ValueUtils.getValue(MaxOccursProperty.getInstance(), element.getProperties());
									// if we have a list and we are doing JSON, we actually want to bind it to the full output because the array is abstracted away in the reponse
									if (maxOccurs != null && maxOccurs != 1) {
										isListBinding = true;
										unmarshallable = new JSONBinding(function.getOutput(), charset);
										// not necessary, they wrap a "value" around the array
	//									((JSONBinding) unmarshallable).setIgnoreRootIfArrayWrapper(true);
										((JSONBinding) unmarshallable).setIgnoreUnknownElements(true); 
									}
									else {
										result = (ComplexType) element.getType();
										resultName = element.getName();
									}
								}
							}
							
							if (unmarshallable == null && result != null) {
								unmarshallable = new JSONBinding(result, charset);
								((JSONBinding) unmarshallable).setIgnoreUnknownElements(true);
							}
							
							if (unmarshallable != null) {
								ComplexContent unmarshal = unmarshallable.unmarshal(IOUtils.toInputStream(readable), new Window[0]);
								// we did the list one, so it _is_ the output
								if (isListBinding) {
									return unmarshal;
								}
								else {
									ComplexContent newInstance = function.getOutput().newInstance();
									newInstance.set(resultName, unmarshal);
									return newInstance;
								}
							}
							return null;
						}
						finally {
							readable.close();
						}
					}
				}
			}
			return null;
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	// TODO: currently if we were using integer keys, we can't actually put a string syntax there!
	// so for integer foreign keys we need to restrict the field and re-add it with a string type in the parser!
	// when we update foreign keys, we need to use a special syntax
	private void scanForForeignKeys(ComplexContent content) {
		// for complex keys we might bind the same thing multiple times which is not too bad in and off itself but is a performance hit
		List<String> alreadyBound = new ArrayList<String>();
		Map<String, NavigationProperty> applicableProperties = new HashMap<String, NavigationProperty>();
		List<NavigationProperty> navigationProperties = definition.getNavigationProperties();
		if (content.getType() instanceof DefinedType) {
			for (NavigationProperty property : navigationProperties) {
				if (property.getQualifiedName().equals(((DefinedType) content.getType()).getId())) {
					applicableProperties.put(property.getElement().getName(), property);
				}
			}
		}
		Collection<Element<?>> allChildren = TypeUtils.getAllChildren((ComplexType) content.getType());
		for (Element<?> child : allChildren) {
			// we have a binding element
			if (child.getName().endsWith("@odata.bind")) {
				// next to the binding string element there should be a complex type that looks like the actual thing
				// the complex type should be used to create a new entry, the bind should be used to bind an existing entity
				String complexName = child.getName().substring(0, child.getName().length() - "@odata.bind".length());
				Element<?> complexBind = content.getType().get(complexName);
				if (complexBind != null) {
					// get the entity-set specific collection name
					String collectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), complexBind.getProperties());
					// if null, we assume there is only one collection for that type, it should be annotated at the global type
					if (collectionName == null) {
						ComplexType globalType = (ComplexType) client.getRepository().resolve(((DefinedType) complexBind.getType()).getId());
						if (globalType != null) {
							collectionName = ValueUtils.getValue(CollectionNameProperty.getInstance(), globalType.getProperties());
						}
					}
					if (collectionName != null) {
						List<Element<?>> linked = new ArrayList<Element<?>>();
						for (Element<?> potential : allChildren) {
							String foreignName = ValueUtils.getValue(ForeignNameProperty.getInstance(), potential.getProperties());
							// linked to this type
							if (foreignName != null && foreignName.equals(complexName)) {
								linked.add(potential);
							}
						}
						if (linked.size() == 1) {
							Object childValue = content.get(linked.get(0).getName());
							if (childValue instanceof Iterable) {
								int counter = 0;
								for (Object singleChild : (Iterable) childValue) {
									content.set(child.getName() + "[" + counter++ + "]", "/" + collectionName + "(" + (singleChild instanceof String ? "'" : "") + singleChild + (singleChild instanceof String ? "'" : "") + ")");
								}
								// unset actual
								content.set(linked.get(0).getName(), null);
							}
							else if (childValue != null) {
								content.set(child.getName(), "/" + collectionName + "(" + (childValue instanceof String ? "'" : "") + childValue + (childValue instanceof String ? "'" : "") + ")");
								// unset actual
								content.set(linked.get(0).getName(), null);
							}
						}
						// no support yet for lists of values!!
						else if (linked.size() > 1) {
							String query = "";
							for (Element<?> bindValue : linked) {
								String foreignKey = ValueUtils.getValue(ForeignKeyProperty.getInstance(), bindValue.getProperties());
								if (foreignKey != null) {
									String [] parts = foreignKey.split(":");
									Object newValue = content.get(bindValue.getName());
									if (newValue != null) {
										if (!query.isEmpty()) {
											query += ",";
										}
										query += parts[1] + "=" + (newValue instanceof String ? "'" : "") + newValue + (newValue instanceof String ? "'" : "");
										// unset actual
										content.set(bindValue.getName(), null);
									}
								}
							}
							if (!query.isEmpty()) {
								content.set(child.getName(), "/" + collectionName + "(" + query + ")");
							}
						}
					}
				}
			}
			else if (child.getType() instanceof ComplexType) {
				Object object = content.get(child.getName());
				if (object instanceof ComplexContent) {
					scanForForeignKeys((ComplexContent) object);
				}
				// TODO: lists as well?
				// TODO: https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/associate-disassociate-entities-using-web-api
			}
		}
	}
	
	// this is inspired by, copied from the jdbc code
	
	public static List<String> inputOperators = Arrays.asList("=", "<>", ">", "<", ">=", "<=", "like", "ilike", "not like", "not ilike");
	// if we have boolean operators, we check if there is a value
	// if it is true, we apply the filter, if it is false, we apply the inverse filter, if it is null, we skip the filter
	// if there is no value, the filter is applied
	// for CRUD this is enforced to be false currently by the crud code
	private static boolean skipFilter(Filter filter) {
		// if it is not a traditional comparison operator, we assume it is a boolean one
		if (!inputOperators.contains(filter.getOperator()) && filter.getValues() != null && !filter.getValues().isEmpty()) {
			Object object = filter.getValues().get(0);
			if (object == null) {
				return true;
			}
		}
		return false;
	}
	private static boolean inverseFilter(Filter filter) {
		if (!inputOperators.contains(filter.getOperator()) && filter.getValues() != null && !filter.getValues().isEmpty()) {
			Object object = filter.getValues().get(0);
			if (object instanceof Boolean && !(Boolean) object) {
				return true;
			}
		}
		return false;
	}
	public static void main(String...args) {
		List<Filter> filters = new ArrayList<Filter>();
		Filter filter = new Filter();
		filter.setKey("accountid");
		filter.setOperator("=");
		filter.setValues(Arrays.asList("a", "b"));
		filters.add(filter);
		System.out.println(new ODataRunner(null).buildFilter(filters));
	}
	private String buildFilter(List<Filter> filters) {
		String where = "";
		boolean openOr = false;
		for (int i = 0; i < filters.size(); i++) {
			Object filterObject = filters.get(i);
			if (filterObject instanceof MaskedContent) {
				filterObject = ((MaskedContent) filterObject).getOriginal();
			}
			if (filterObject instanceof BeanInstance) {
				filterObject = ((BeanInstance<?>) filterObject).getUnwrapped();
			}
			Filter filter = (Filter) filterObject;
			if (filter.getKey() == null) {
				continue;
			}
			
			if (skipFilter(filter)) {
				continue;
			}
			
			if (!where.isEmpty()) {
				if (filter.isOr()) {
					where += " or";
				}
				else {
					where += " and";
				}
			}
			Filter nextFilter = null;
			if (i < filters.size() - 1) {
				Object nextFilterObject = filters.get(i + 1);
				if (nextFilterObject instanceof MaskedContent) {
					nextFilterObject = ((MaskedContent) nextFilterObject).getOriginal();
				}
				if (nextFilterObject instanceof BeanInstance) {
					nextFilterObject = ((BeanInstance<?>) nextFilterObject).getUnwrapped();
				}
				nextFilter = (Filter) nextFilterObject;
			}
			// start the or
			if (!openOr && nextFilter != null && nextFilter.isOr()) {
				where += " (";
				openOr = true;
			}
			
			boolean inverse = inverseFilter(filter);
			String operator = filter.getOperator();
			
			if (filter.getValues() != null && filter.getValues().size() >= 2) {
				if (operator.equals("=")) {
					operator = "in";
				}
				else if (operator.equals("<>")) {
					operator = "in";
					inverse = true;
				}
			}
			
			if (operator.toLowerCase().equals("not like") || operator.toLowerCase().equals("not ilike")) {
				operator = "like";
				inverse = true;
			}
			
			if (inverse && operator.toLowerCase().equals("is null")) {
				operator = "is not null";
				inverse = false;
			}
			else if (inverse && operator.toLowerCase().equals("is not null")) {
				operator = "is null";
				inverse = false;
			}
			else if (inverse) {
				where += " not(";
			}
			
			if (operator.equals("like")) {
				where += "contains(";
			}
			if (filter.isCaseInsensitive()) {
				where += " tolower(" + filter.getKey() + ")";
			}
			else {
				where += " " + filter.getKey();
			}
			where += " " + mapOperator(operator);
			
			if (filter.getValues() != null && !filter.getValues().isEmpty() && (inputOperators.contains(operator) || "in".equals(operator))) {
				if (filter.getValues().size() == 1) {
					Object object = filter.getValues().get(0);
					
					// for the like operator, we have probably injected "%" to indicate wildcards, remove those, there are no wildcards here
					if (operator.equals("like")) {
						object = object.toString().replace("%", "");
					}
					if (filter.isCaseInsensitive()) {
						where += " tolower('" + object + "')";
					}
					else if (object instanceof Date) {
						// we want classic dateTime formatting of the date, not the default java stringification
						String stringifiedDate = ConverterFactory.getInstance().getConverter().convert(object, String.class);
						// timezone is mandatory!
						where += " " + stringifiedDate + "Z";
					}
					else {
						where += " " + (object instanceof String ? "'" + object + "'" : object);
					}
				}
				else {
					where += " (";
					boolean first = true;
					for (Object single : filter.getValues()) {
						if (first) {
							first = false;
						}
						else {
							where += ",";
						}
						if (filter.isCaseInsensitive()) {
							where += "tolower('" + single + "')";
						}
						else if (single instanceof Date) {
							// we want classic dateTime formatting of the date, not the default java stringification
							String stringifiedDate = ConverterFactory.getInstance().getConverter().convert(single, String.class);
							// timezone is mandatory!
							where += " " + stringifiedDate + "Z";
						}
						else {
							where += (single instanceof String ? "'" + single + "'" : single);
						}
					}
					where += ")";
				}
			}
			
			if (operator.equals("like")) {
				where += ")";
			}
			
			// close the not statement
			if (inverse) {
				where += ")";
			}
			// check if we want to close an or
			if (nextFilter != null && openOr && !nextFilter.isOr()) {
				where += ")";
				openOr = false;
			}
		}
		if (openOr) {
			where += ")";
			openOr = false;
		}
		return where;
	}
	
	private String mapOperator(String operator) {
		if ("=".equals(operator)) {
			return "eq";
		}
		else if ("!=".equals(operator) || "<>".equals(operator)) {
			return "ne";
		}
		else if (">".equals(operator)) {
			return "gt";
		}
		else if (">=".equals(operator)) {
			return "ge";
		}
		else if ("<".equals(operator)) {
			return "lt";
		}
		else if ("<=".equals(operator)) {
			return "le";
		}
		else if ("&&".equals(operator)) {
			return "and";
		}
		else if ("||".equals(operator)) {
			return "or";
		}
		// we map the like to a contains() function! so we just need a separator
		else if ("like".equals(operator)) {
			return ",";
		}
		else if ("is null".equals(operator)) {
			return "eq null";
		}
		else if ("is not null".equals(operator)) {
			return "ne null";
		}
		return operator;
	}
}
