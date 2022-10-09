package be.nabu.eai.module.odata.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import be.nabu.eai.module.types.structure.StructureManager;
import be.nabu.eai.repository.EAINode;
import be.nabu.eai.repository.EAIRepositoryUtils;
import be.nabu.eai.repository.api.ArtifactRepositoryManager;
import be.nabu.eai.repository.api.Entry;
import be.nabu.eai.repository.api.ModifiableEntry;
import be.nabu.eai.repository.api.Repository;
import be.nabu.eai.repository.managers.base.JAXBArtifactManager;
import be.nabu.eai.repository.resources.MemoryEntry;
import be.nabu.libs.artifacts.api.Artifact;
import be.nabu.libs.odata.ODataDefinition;
import be.nabu.libs.odata.types.Function;
import be.nabu.libs.resources.api.ResourceContainer;
import be.nabu.libs.services.api.DefinedService;
import be.nabu.libs.types.api.ComplexType;
import be.nabu.libs.types.api.DefinedType;
import be.nabu.libs.types.api.SimpleType;

public class ODataClientManager extends JAXBArtifactManager<ODataClientConfiguration, ODataClient> implements ArtifactRepositoryManager<ODataClient> {

	public ODataClientManager() {
		super(ODataClient.class);
	}

	@Override
	public List<Entry> addChildren(ModifiableEntry root, ODataClient artifact) throws IOException {
		List<Entry> entries = new ArrayList<Entry>();
		((EAINode) root.getNode()).setLeaf(false);
		List<String> entitySets = artifact.getConfig().getEntitySets();
		Set<String> usedTypes = new HashSet<String>();
		boolean showAll = false;
		if (entitySets != null || showAll) {
			ODataDefinition definition = artifact.getDefinition();
			List<Function> functions = definition.getFunctions();
			if (functions != null) {
				for (Function function : functions) {
					String entitySet = (function.getContext() == null ? "" : function.getContext() + ".") + function.getName();
					// if the function has an entity context, check if it wants to be exposed
					if (function.getContext() != null) {
						if (showAll || entitySets.indexOf(function.getContext()) >= 0) {
							ODataClientService child = new ODataClientService(root.getId() + ".services." + entitySet, artifact, function);
							addChild(root, artifact, entries, child);
							if (!showAll) {
								usedTypes.addAll(getUsedTypes(child));
							}
						}
					}
				}
			}
		}
		for (String namespace : artifact.getDefinition().getRegistry().getNamespaces()) {
			for (ComplexType type : artifact.getDefinition().getRegistry().getComplexTypes(namespace)) {
				if (type instanceof DefinedType) {
					if (showAll || usedTypes.contains(((DefinedType) type).getId())) {
						addChild(root, artifact, entries, (DefinedType) type);
					}
				}
			}
			for (SimpleType<?> type : artifact.getDefinition().getRegistry().getSimpleTypes(namespace)) {
				if (type instanceof DefinedType) {
					if (showAll || usedTypes.contains(((DefinedType) type).getId())) {
						addChild(root, artifact, entries, (DefinedType) type);
					}
				}
			}
		}
		return entries;
	}
	
	private Set<String> getUsedTypes(DefinedService service) {
		Set<String> set = new HashSet<String>();
		set.addAll(StructureManager.getComplexReferences(service.getServiceInterface().getInputDefinition(), true));
		set.addAll(StructureManager.getComplexReferences(service.getServiceInterface().getOutputDefinition(), true));
		return set;
	}
	
	private void addChild(ModifiableEntry root, ODataClient artifact, List<Entry> entries, Artifact child) {
		String id = child.getId();
		if (id.startsWith(artifact.getId() + ".")) {
			String parentId = id.replaceAll("\\.[^.]+$", "");
			ModifiableEntry parent = EAIRepositoryUtils.getParent(root, id.substring(artifact.getId().length() + 1), false);
			EAINode node = new EAINode();
			node.setArtifact(child);
			node.setLeaf(true);
			MemoryEntry entry = new MemoryEntry(artifact.getId(), root.getRepository(), parent, node, id, id.substring(parentId.length() + 1));
			node.setEntry(entry);
			parent.addChildren(entry);
			entries.add(entry);
		}
	}

	@Override
	public List<Entry> removeChildren(ModifiableEntry parent, ODataClient artifact) throws IOException {
		List<Entry> entries = new ArrayList<Entry>();
		Entry services = parent.getChild("services");
		if (services != null) {
			for (Entry service : services) {
				entries.add(service);
			}
			parent.removeChildren("services");
		}
		Entry types = parent.getChild("types");
		if (types != null) {
			for (Entry type : types) {
				entries.add(type);
			}
			parent.removeChildren("types");
		}
		return entries;
	}

	@Override
	protected ODataClient newInstance(String id, ResourceContainer<?> container, Repository repository) {
		return new ODataClient(id, container, repository);
	}

}
