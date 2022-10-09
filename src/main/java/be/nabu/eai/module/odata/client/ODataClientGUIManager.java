package be.nabu.eai.module.odata.client;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import be.nabu.eai.developer.MainController;
import be.nabu.eai.developer.managers.base.BaseJAXBGUIManager;
import be.nabu.eai.developer.managers.util.SimpleProperty;
import be.nabu.eai.developer.managers.util.SimplePropertyUpdater;
import be.nabu.eai.developer.util.EAIDeveloperUtils;
import be.nabu.eai.repository.resources.RepositoryEntry;
import be.nabu.jfx.control.ace.AceEditor;
import be.nabu.libs.http.HTTPException;
import be.nabu.libs.http.api.HTTPRequest;
import be.nabu.libs.http.api.HTTPResponse;
import be.nabu.libs.http.api.client.HTTPClient;
import be.nabu.libs.http.core.DefaultHTTPRequest;
import be.nabu.libs.odata.types.Function;
import be.nabu.libs.property.api.Property;
import be.nabu.libs.property.api.Value;
import be.nabu.libs.resources.URIUtils;
import be.nabu.libs.resources.api.ManageableContainer;
import be.nabu.libs.resources.api.ReadableResource;
import be.nabu.libs.resources.api.Resource;
import be.nabu.libs.resources.api.WritableResource;
import be.nabu.libs.types.base.ValueImpl;
import be.nabu.utils.io.IOUtils;
import be.nabu.utils.io.api.ByteBuffer;
import be.nabu.utils.io.api.ReadableContainer;
import be.nabu.utils.io.api.WritableContainer;
import be.nabu.utils.mime.api.ContentPart;
import be.nabu.utils.mime.impl.MimeHeader;
import be.nabu.utils.mime.impl.PlainMimeEmptyPart;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.value.ChangeListener;
import javafx.beans.value.ObservableValue;
import javafx.event.ActionEvent;
import javafx.event.EventHandler;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.control.Accordion;
import javafx.scene.control.Button;
import javafx.scene.control.CheckBox;
import javafx.scene.control.Label;
import javafx.scene.control.ScrollPane;
import javafx.scene.control.Tab;
import javafx.scene.control.TextField;
import javafx.scene.control.TitledPane;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;

public class ODataClientGUIManager extends BaseJAXBGUIManager<ODataClientConfiguration, ODataClient> {

	public ODataClientGUIManager() {
		super("OData Client", ODataClient.class, new ODataClientManager(), ODataClientConfiguration.class);
	}
	
	@Override
	public String getCategory() {
		return "REST";
	}

	protected List<Property<?>> getCreateProperties() {
		return null;
	}

	@Override
	protected ODataClient newInstance(MainController controller, RepositoryEntry entry, Value<?>...values) throws IOException {
		return new ODataClient(entry.getId(), entry.getContainer(), entry.getRepository());
	}

	@Override
	public void display(MainController controller, AnchorPane pane, ODataClient instance) {
		ScrollPane scroll = new ScrollPane();
		AnchorPane.setBottomAnchor(scroll, 0d);
		AnchorPane.setTopAnchor(scroll, 0d);
		AnchorPane.setLeftAnchor(scroll, 0d);
		AnchorPane.setRightAnchor(scroll, 0d);
		VBox vbox = new VBox();
		HBox buttons = new HBox();
		buttons.setPadding(new Insets(10));
		buttons.setAlignment(Pos.CENTER);
		vbox.getChildren().add(buttons);
		Button upload = new Button("Update from file");
		Button download = new Button("Update from URI");
		Button view = new Button("View Metadata");
		buttons.getChildren().addAll(upload, download, view);
		scroll.setContent(vbox);
		scroll.setFitToWidth(true);
		
		AnchorPane anchorPane = new AnchorPane();
		Accordion accordion = displayWithAccordion(instance, anchorPane);
		
		vbox.getChildren().add(anchorPane);
		
		if (instance.getDefinition() != null) {
			try {
				VBox drawEntities = drawEntitySets(instance);
			
				// allow choosing of exposed operations
				TitledPane entitySets = new TitledPane("Entity Sets", drawEntities);
				accordion.getPanes().add(0, entitySets);
				
				// we want this as the default
				accordion.setExpandedPane(entitySets);
			}
			catch (Exception e) {
				MainController.getInstance().notify(e);
			}
		}
		
		// TODO: draw additional functions (both global functions and for selected entities)
		
		pane.getChildren().add(scroll);
		
		upload.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void handle(ActionEvent arg0) {
				SimpleProperty<File> fileProperty = new SimpleProperty<File>("File", File.class, true);
				fileProperty.setInput(true);
				final SimplePropertyUpdater updater = new SimplePropertyUpdater(true, new LinkedHashSet(Arrays.asList(new Property [] { fileProperty })));
				EAIDeveloperUtils.buildPopup(MainController.getInstance(), updater, "OData Metadata File", new EventHandler<ActionEvent>() {
					@Override
					public void handle(ActionEvent arg0) {
						try {
							if (instance.getDirectory().getChild("odata-metadata.xml") != null) {
								((ManageableContainer<?>) instance.getDirectory()).delete("odata-metadata.xml");
							}
							File file = updater.getValue("File");
							if (file != null) {
								Resource child = instance.getDirectory().getChild("odata-metadata.xml");
								if (child == null) {
									child = ((ManageableContainer<?>) instance.getDirectory()).create("odata-metadata.xml", "application/xml");
								}
								WritableContainer<ByteBuffer> writable = ((WritableResource) child).getWritable();
								try {
									InputStream input = new BufferedInputStream(new FileInputStream(file));
									try {
										IOUtils.copyBytes(IOUtils.wrap(input), writable);
									}
									finally {
										input.close();
									}
								}
								finally {
									writable.close();
								}
							}
							MainController.getInstance().setChanged();
							MainController.getInstance().refresh(instance.getId());
						}
						catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				});
			}
		});
		download.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@SuppressWarnings({ "rawtypes", "unchecked" })
			@Override
			public void handle(ActionEvent arg0) {
				SimpleProperty<URI> fileProperty = new SimpleProperty<URI>("URI", URI.class, true);
				// just set the existing as default
				final SimplePropertyUpdater updater = new SimplePropertyUpdater(true, new LinkedHashSet(Arrays.asList(new Property [] { fileProperty })), new ValueImpl<URI>(fileProperty, instance.getConfig().getEndpoint()));
				EAIDeveloperUtils.buildPopup(MainController.getInstance(), updater, "OData Endpoint", new EventHandler<ActionEvent>() {
					@Override
					public void handle(ActionEvent arg0) {
						try {
							if (instance.getDirectory().getChild("odata-metadata.xml") != null) {
								((ManageableContainer<?>) instance.getDirectory()).delete("odata-metadata.xml");
							}
							URI uri = updater.getValue("URI");
							if (uri != null) {
								Resource child = instance.getDirectory().getChild("odata-metadata.xml");
								if (child == null) {
									child = ((ManageableContainer<?>) instance.getDirectory()).create("odata-metadata.xml", "application/xml");
								}
								WritableContainer<ByteBuffer> writable = ((WritableResource) child).getWritable();
								try {
									InputStream stream = getMetadata(instance, uri);
									try {
										IOUtils.copyBytes(IOUtils.wrap(stream), writable);
									}
									finally {
										stream.close();
									}
								}
								finally {
									writable.close();
								}
								instance.getConfig().setEndpoint(uri);
								// save it, otherwise it is not persisted it seems
								MainController.getInstance().save(instance.getId());
							}
							MainController.getInstance().refresh(instance.getId());
						}
						catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				});
			}
		});
		view.addEventHandler(ActionEvent.ANY, new EventHandler<ActionEvent>() {
			@Override
			public void handle(ActionEvent arg0) {
				Resource swagger = instance.getDirectory().getChild("odata-metadata.xml");
				if (swagger != null) {
					try {
						ReadableContainer<ByteBuffer> readable = ((ReadableResource) swagger).getReadable();
						try {
							String tabId = instance.getId() + " (metadata)";
							Tab tab = MainController.getInstance().getTab(tabId);
							if (tab == null) {
								tab = MainController.getInstance().newTab(tabId);
								AceEditor editor = new AceEditor();
								editor.setContent("application/xml", new String(IOUtils.toBytes(readable), "UTF-8"));
								editor.setReadOnly(true);
								tab.setContent(editor.getWebView());
							}
							MainController.getInstance().activate(tabId);
						}
						finally {
							readable.close();
						}
					}
					catch (IOException e) {
						MainController.getInstance().notify(e);
					}
				}
			}
		});
	}

	public InputStream getMetadata(ODataClient client, URI url) {
		URI child = URIUtils.getChild(url, "$metadata");
		HTTPClient httpClient = client.getParser().getHTTPClient();
		HTTPRequest request = new DefaultHTTPRequest("GET", child.getPath(), new PlainMimeEmptyPart(null, 
			new MimeHeader("Content-Length", "0"),
			new MimeHeader("Accept", "application/xml"),
			new MimeHeader("User-Agent", "User agent"),
			new MimeHeader("Host", child.getHost())
		));
		// TODO: we could use the security here as well...?
		// but this is client side, maybe we need to push this to server side?
		try {
			HTTPResponse response = httpClient.execute(request, null, child.getScheme().equals("https"), true);
			if (response.getCode() >= 200 && response.getCode() < 300) {
				if (response.getContent() instanceof ContentPart) {
					ReadableContainer<ByteBuffer> readable = ((ContentPart) response.getContent()).getReadable();
					return IOUtils.toInputStream(readable);
				}
				else {
					throw new IllegalStateException("The response does not contain any content");
				}
			}
			else {
				throw new HTTPException(response.getCode());
			}
		}
		catch (RuntimeException e) {
			throw e;
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private VBox drawEntitySets(ODataClient instance) {
		VBox entitySets = new VBox();
		TextField filter = new TextField();
		filter.setPromptText("Search");
		entitySets.getChildren().add(filter);
		VBox.setMargin(filter, new Insets(10));
		List<Function> functions = instance.getDefinition().getFunctions();
		List<String> available = new ArrayList<String>();
		for (Function function : functions) {
			if (function.getContext() != null && !available.contains(function.getContext())) {
				available.add(function.getContext());
			}
		}
		Map<String, BooleanProperty> entityBooleanMap = new HashMap<String, BooleanProperty>();
		Map<String, CheckBox> entityCheckboxMap = new HashMap<String, CheckBox>();
		
		for (String single : available) {
			HBox box = new HBox();
			box.setAlignment(Pos.CENTER_LEFT);
			CheckBox checkBox = new CheckBox();
			
			Label entityLabel = new Label(single);
			HBox.setMargin(entityLabel, new Insets(0, 10, 0, 10));
			
			checkBox.setSelected(instance.getConfig().getEntitySets() != null && instance.getConfig().getEntitySets().indexOf(single) >= 0);
			entityCheckboxMap.put(single, checkBox);
			box.getChildren().addAll(checkBox, entityLabel);
			entitySets.getChildren().add(box);
			
			checkBox.selectedProperty().addListener(new ChangeListener<Boolean>() {
				@Override
				public void changed(ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) {
					// if it is disabled, we might be manipulating it from somewhere else, don't persist the value!
					if (!checkBox.isDisabled()) {
						// if we didn't set it or set it to true, we expose all, makes no sense to change it
						if (newValue != null && newValue) {
							if (instance.getConfig().getEntitySets() == null) {
								instance.getConfig().setEntitySets(new ArrayList<String>());
							}
							if (instance.getConfig().getEntitySets().indexOf(single) < 0) {
								instance.getConfig().getEntitySets().add(single);
								MainController.getInstance().setChanged();
							}
						}
						else if (instance.getConfig().getEntitySets() != null) {
							int indexOf = instance.getConfig().getEntitySets().indexOf(single);
							if (indexOf >= 0) {
								instance.getConfig().getEntitySets().remove(indexOf);
								MainController.getInstance().setChanged();
							}
						}
					}
				}
			});
			
			entityBooleanMap.put(single, new SimpleBooleanProperty(true));
			box.visibleProperty().bind(entityBooleanMap.get(single));
			box.managedProperty().bind(box.visibleProperty());
			box.setPadding(new Insets(10));
		}
		filter.textProperty().addListener(new ChangeListener<String>() {
			@Override
			public void changed(ObservableValue<? extends String> observable, String oldValue, String newValue) {
				if (newValue == null || newValue.trim().isEmpty()) {
					for (BooleanProperty property : entityBooleanMap.values()) {
						property.set(true);
					}
				}
				else {
					String regex = newValue.contains("*") ? "(?i)(?s).*" + newValue.replace("*", ".*") + ".*" : null;
					for (String single : available) {
						if (regex != null) {
							entityBooleanMap.get(single).set(single.matches(regex));
						}
						else {
							entityBooleanMap.get(single).set(single.toLowerCase().indexOf(newValue.toLowerCase()) >= 0);
						}
					}
				}
			}
		});
		return entitySets;
	}
	
}
