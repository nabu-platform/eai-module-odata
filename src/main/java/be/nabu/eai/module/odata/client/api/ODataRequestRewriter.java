package be.nabu.eai.module.odata.client.api;

import javax.jws.WebParam;

import be.nabu.libs.http.api.HTTPRequest;

public interface ODataRequestRewriter {
	public void rewrite(@WebParam(name = "odataClientId") String odataClientId, @WebParam(name = "request") HTTPRequest request);
}
