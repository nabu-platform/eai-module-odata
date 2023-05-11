package be.nabu.eai.module.odata.client;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import be.nabu.eai.module.odata.client.api.ODataRequestRewriter;
import be.nabu.eai.repository.util.Filter;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.core.HTTPRequestAuthenticatorFactory;
import be.nabu.libs.http.core.HTTPUtils;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.parser.ODataExpansion;
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
import be.nabu.libs.types.binding.api.MarshallableBinding;
import be.nabu.libs.types.binding.api.UnmarshallableBinding;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.mask.MaskedContent;
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
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public ComplexContent run(Function function, ComplexContent input) {
		try {
			Object transactionId = input == null ? null : input.get("transactionId");
			
			String target = definition.getBasePath();
			// the context is set to the entity set name
			target += "/" + function.getContext();
			ComplexContent functionInput = null;
			for (Element<?> element : TypeUtils.getAllChildren(function.getInput())) {
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
			if (filter == null) {
				List<Filter> filters = input == null ? null : (List<Filter>) input.get("filters");
				if (filters != null && !filters.isEmpty()) {
					filter = buildFilter(filters);
				}
			}
			if (filter != null) {
				if (queryBegun) {
					target += "&";
				}
				else {
					queryBegun = true;
					target += "?";
				}
				target += "$filter=" + URIUtils.encodeURL(filter);
			}
				
			Charset charset = client.getConfig().getCharset();
			if (charset == null) {
				charset = Charset.forName("UTF-8");
			}
			
			// if we are getting, we need to keep track of expansion
			// we use the duplicate property for that
			if ("GET".equalsIgnoreCase(function.getMethod())) {
				String expand = null;
				for (Element<?> child : TypeUtils.getAllChildren(function.getOutput())) {
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
			HTTPResponse response = client.execute(request, null, "https".equals(definition.getScheme()), true);
			HTTPUtils.validateResponse(response);
			// we did a create and want to check for a header that indicates the id of
			if (response.getCode() == 204 && "POST".equalsIgnoreCase(function.getMethod())) {
				Header header = MimeUtils.getHeader("OData-EntityId", response.getContent().getHeaders());
				if (header == null) {
					header = MimeUtils.getHeader("Location", response.getContent().getHeaders());
				}
				if (header != null) {
					// check if there is a field to put it in
					Collection<Element<?>> allChildren = TypeUtils.getAllChildren(function.getOutput());
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
						for (Element<?> element : TypeUtils.getAllChildren(function.getOutput())) {
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
	
	public static List<String> inputOperators = Arrays.asList("=", "<>", ">", "<", ">=", "<=", "like", "ilike");
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
			// start the or
			if (i < filters.size() - 1 && !openOr && filters.get(i + 1).isOr()) {
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
			if (i < filters.size() - 1 && openOr && !filters.get(i + 1).isOr()) {
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
