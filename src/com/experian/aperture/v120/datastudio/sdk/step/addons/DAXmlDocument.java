package com.experian.aperture.v120.datastudio.sdk.step.addons;

import java.util.ArrayList;

import javax.wsdl.Definition;
import javax.wsdl.Types;
import javax.wsdl.extensions.schema.Schema;
import javax.wsdl.factory.WSDLFactory;
import javax.wsdl.xml.WSDLReader;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class DAXmlDocument {
	final String CONTROL_DATASOURCE_TAG_NAME = "OCONTROL";
	final String INPUT_DATASOURCE_TAG_NAME = "INPUT";
	final String RESULTs_DATASOURCE_TAG_NAME = "RESULTS";

	Element mainDocEl = null;

	private ArrayList<String> controlDataSourceList = new ArrayList<String>();
	private ArrayList<String> inputDataSourceList = new ArrayList<String>();
	private ArrayList<String> resultsDataSourceList = new ArrayList<String>();

	public DAXmlDocument(String wsdlpathstring) throws Exception {
		initialize( wsdlpathstring);
	}

	public void initialize(String wsdlpathstring) throws Exception {
		WSDLFactory fac = WSDLFactory.newInstance();			
		WSDLReader readwsdl = fac.newWSDLReader();
		//readwsdl.setFeature("javax.wsdl.verbose", false);
		Definition wsdlInstance = readwsdl.readWSDL(null, wsdlpathstring);

		Types types = wsdlInstance.getTypes();
		Schema schema = (Schema) types.getExtensibilityElements().get(0);

		Element el = (Element) schema .getElement();

		mainDocEl = el;

		//		System.out.println("XX: " + schema.getElement().getLastChild().toString());
		NodeList nodeList = el.getChildNodes();
		Element el2 = getElementByAttributeValue(nodeList, "name", "DAXMLDocument");
		NodeList nl2 = el2.getChildNodes();
		Element el3 = getElementByAttributeValue(nl2, "name", "OCONTROL");
		NodeList nl3 = el3.getChildNodes();
		Element el4 = getElementByTagName(nl3, "xs:all");
		extractOCONTROLElementName(el4);

		el3 = getElementByAttributeValue(nl2, "name", "INPUT");
		nl3 = el3.getChildNodes();
		el4 = getElementByTagName(nl3, "xs:all");
		extractINPUTElementName(el4);

		el3 = getElementByAttributeValue(nl2, "name", "RESULTS");
		nl3 = el3.getChildNodes();
		el4 = getElementByTagName(nl3, "xs:all");
		extractRESULTSElementName(el4);
		//System.out.println(el2.getChildNodes().getLength());
	}

	private void extractOCONTROLElementName(Element el) {
		NodeList nl = el.getChildNodes();
		System.out.println("Extracting OCONTROL : " + nl.getLength());
		for(int a=0;a<nl.getLength();a++) {
			Object obj = nl.item(a);
			if (obj != null && obj instanceof Element) {
				String nodeName = nl.item(a).getNodeName();
				if (nodeName != null && nodeName.equals("xs:element")) {
					Element elDAXmlDoc = (Element) nl.item(a);
					getControlDataSourceList().add(elDAXmlDoc.getAttribute("name"));
					//					System.out.println("OCONTROL ATTRIBUTE : " + elDAXmlDoc.getAttribute("name"));
				}
			}
		}
	}

	private void extractINPUTElementName(Element el) {
		NodeList nl = el.getChildNodes();
		System.out.println("Extracting INPUT : " + nl.getLength());
		for(int a=0;a<nl.getLength();a++) {
			Object obj = nl.item(a);
			if (obj != null && obj instanceof Element) {
				String nodeName = nl.item(a).getNodeName();
				if (nodeName != null && nodeName.equals("xs:element")) {
					Element elDAXmlDoc = (Element) nl.item(a);
					getInputDataSourceList().add(elDAXmlDoc.getAttribute("name"));
					//					System.out.println("INPUT ATTRIBUTE : " + elDAXmlDoc.getAttribute("name"));
				}
			}
		}
	}

	private void extractRESULTSElementName(Element el) {
		NodeList nl = el.getChildNodes();
		System.out.println("Extracting RESULTS : " + nl.getLength());
		for(int a=0;a<nl.getLength();a++) {
			Object obj = nl.item(a);
			if (obj != null && obj instanceof Element) {
				String nodeName = nl.item(a).getNodeName();
				if (nodeName != null && nodeName.equals("xs:element")) {
					Element elDAXmlDoc = (Element) nl.item(a);
					getResultsDataSourceList().add(elDAXmlDoc.getAttribute("name"));
					//					System.out.println("RESULTS ATTRIBUTE : " + elDAXmlDoc.getAttribute("name"));
				}
			}
		}
	}

	public Element getElementByTagName(String tagname) {
		return getElementByTagName(mainDocEl.getChildNodes(), tagname);
	}

	public Element getElementByTagName(NodeList nl, String tagname) {
		tagname = tagname == null ? "name" : tagname;
		System.out.println("Length : " + nl.getLength());

		if (nl == null || tagname == null)
			return null;

		for(int a=0;a<nl.getLength();a++) {
			Object obj = nl.item(a);
			if (obj != null && obj instanceof Element) {
				String nodeName = nl.item(a).getNodeName();
				System.out.println("Loop : " + nodeName);
				if (nodeName != null && nodeName.equals(tagname)) {
					Element elDAXmlDoc = (Element) nl.item(a);

					return elDAXmlDoc;
				} else if (nl.item(a).hasChildNodes()) {
					NodeList nlsub = nl.item(a).getChildNodes();
					Element result = getElementByTagName(nlsub, tagname);
					if (result != null && result.getNodeName().equals(tagname)) 
						return result;
				}
			}
		}

		return null;
	}

	public Element getElementByAttributeValue(String attrname, String attrvalue) {
		return getElementByAttributeValue(mainDocEl.getChildNodes(), attrname, attrvalue);
	}

	public Element getElementByAttributeValue(NodeList nl, String attrname, String attrvalue) {
		attrname = attrname == null ? "name" : attrname;
		//		boolean found = false;

		if (nl == null || attrvalue == null)
			return null;

		for(int a=0;a<nl.getLength();a++) {
			Object obj = nl.item(a);
			if (obj != null && obj instanceof Element) {
				String nodeName = nl.item(a).getNodeName();
				if (nodeName != null && nodeName.equals("xs:element")) {
					Element elDAXmlDoc = (Element) nl.item(a);
					//System.out.println("eldaxname : " + elDAXmlDoc.getAttribute("name"));
					if (elDAXmlDoc.getAttribute(attrname).equals(attrvalue)) {
						return elDAXmlDoc;
					} else if (elDAXmlDoc.hasChildNodes()) {
						NodeList nlsub = elDAXmlDoc.getChildNodes();
						Element result = getElementByAttributeValue(nlsub, attrname, attrvalue);
						if (result != null && result.getAttribute(attrname).equals(attrvalue)) 
							return result;
					}
				}
				else if (nodeName != null && (nodeName.equals("xs:complexType")) || nodeName.equals("xs:all")) {
					NodeList nlsub = nl.item(a).getChildNodes();
					Element result = getElementByAttributeValue(nlsub, attrname, attrvalue);
					if (result != null && result.getAttribute(attrname).equals(attrvalue)) 
						return result;
				}
			}
		}

		//		System.out.println("Not found !");
		return null;
	}

	public ArrayList<String> getControlDataSourceList() {
		return controlDataSourceList;
	}

	void setControlDataSourceList(ArrayList<String> controlDataSourceList) {
		this.controlDataSourceList = controlDataSourceList;
	}

	public ArrayList<String> getInputDataSourceList() {
		return inputDataSourceList;
	}

	void setInputDataSourceList(ArrayList<String> inputDataSourceList) {
		this.inputDataSourceList = inputDataSourceList;
	}

	public ArrayList<String> getResultsDataSourceList() {
		return resultsDataSourceList;
	}

	void setResultsDataSourceList(ArrayList<String> resultsDataSourceList) {
		this.resultsDataSourceList = resultsDataSourceList;
	}	


}
