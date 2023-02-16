package be.nabu.eai.module.odata.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import be.nabu.eai.developer.api.InterfaceLister;
import be.nabu.eai.developer.util.InterfaceDescriptionImpl;

public class ODataInterfaceLister implements InterfaceLister {

	private static Collection<InterfaceDescription> descriptions = null;
	
	@Override
	public Collection<InterfaceDescription> getInterfaces() {
		if (descriptions == null) {
			synchronized(ODataInterfaceLister.class) {
				if (descriptions == null) {
					List<InterfaceDescription> descriptions = new ArrayList<InterfaceDescription>();
					descriptions.add(new InterfaceDescriptionImpl("OData", "Client Request Rewriter", "be.nabu.eai.module.odata.client.api.ODataRequestRewriter.rewrite"));
					ODataInterfaceLister.descriptions = descriptions;
				}
			}
		}
		return descriptions;
	}

}
