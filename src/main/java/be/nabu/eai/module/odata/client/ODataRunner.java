package be.nabu.eai.module.odata.client;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import be.nabu.eai.repository.util.Filter;
import be.nabu.libs.http.HTTPException;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.http.core.HTTPRequestAuthenticatorFactory;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.types.Function;
import be.nabu.libs.property.ValueUtils;
import be.nabu.libs.services.ServiceRuntime;
import be.nabu.libs.types.TypeUtils;
import be.nabu.libs.types.api.ComplexContent;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.Element;
import be.nabu.libs.types.api.Marshallable;
import be.nabu.libs.types.api.SimpleType;
import be.nabu.libs.types.binding.api.MarshallableBinding;
import be.nabu.libs.types.binding.api.UnmarshallableBinding;
import be.nabu.libs.types.binding.api.Window;
import be.nabu.libs.types.binding.json.JSONBinding;
import be.nabu.libs.types.java.BeanInstance;
import be.nabu.libs.types.mask.MaskedContent;
import be.nabu.libs.types.properties.MaxOccursProperty;
import be.nabu.libs.types.properties.PrimaryKeyProperty;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.api.ModifiablePart;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeContentPart;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;
import nabu.protocols.http.client.Services;

public class ODataRunner {
	private ODataDefinition definition;
	private ODataClient client;

	public ODataRunner(ODataClient client) {
		this.client = client;
		this.definition = client.getDefinition();
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
				target += "$search=" + search;
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
					target += single;
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
				target += "$filter=" + filter;
			}
				
			Charset charset = client.getConfig().getCharset();
			if (charset == null) {
				charset = Charset.forName("UTF-8");
			}
			
			ModifiablePart part = null;
			byte [] content = null;
			if (functionInput != null) {
				MarshallableBinding binding = null;
				// currently only json
				String contentType = "application/json";
				
				if ("application/json".equals(contentType)) {
					binding = new JSONBinding(functionInput.getType(), charset);
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
				part.setHeader(new MimeHeader("If-Match", "*"));
			}
			
			DefaultHTTPRequest request = new DefaultHTTPRequest(function.getMethod(), target, part);
			
			if (client.getConfig().getSecurityType() != null) {
				if (!HTTPRequestAuthenticatorFactory.getInstance().getAuthenticator(client.getConfig().getSecurityType())
					.authenticate(request, client.getConfig().getSecurityContext(), null, false)) {
					throw new IllegalStateException("Could not authenticate the request");
				}
			}
			HTTPClient client = Services.getTransactionable(ServiceRuntime.getRuntime().getExecutionContext(), transactionId == null ? null : transactionId.toString(), this.client.getConfig().getHttpClient()).getClient();
			HTTPResponse response = client.execute(request, null, "https".equals(definition.getScheme()), true);
			if (response.getCode() < 200 || response.getCode() >= 300) {
				throw new HTTPException(response.getCode());
			}
			if (response.getContent() instanceof ContentPart) {
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
			
			if (filter.getValues() != null && !filter.getValues().isEmpty() && inputOperators.contains(operator)) {
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
							where += ", ";
						}
						if (filter.isCaseInsensitive()) {
							where += " tolower('" + single + "')";
						}
						else {
							where += " " + (single instanceof String ? "'" + single + "'" : single);
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
		return null;
	}
}
