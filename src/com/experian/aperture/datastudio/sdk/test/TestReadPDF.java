package com.experian.aperture.datastudio.sdk.test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.text.TextPosition;

import com.experian.aperture.datastudio.sdk.step.addons.MyPDFTextParser;

public class TestReadPDF extends PDFTextStripper {
	public TestReadPDF() throws IOException {
		super();
		// TODO Auto-generated constructor stub
	}

	static List<String> lines = new ArrayList<String>();
	
	protected void writeString(String str, List<TextPosition> textPositions) throws IOException {
        
        
        String name = "";
        
        Pattern patternNameAnchor = Pattern.compile("[\\w]{2}[\\w].[\\d][\\d][\\d] Name:");
        Pattern patternName = Pattern.compile("([\\d]: [\\w'\\-\\ ]+)(( \\b)|$)");
        Matcher matcher = patternNameAnchor.matcher(str);
        if (matcher.find())
        {
        	System.out.println(str);
        	//matcher.reset();
        	Matcher matcher2 = patternName.matcher(str);
//        	System.out.println(matcher2.groupCount());
        	while(matcher2.find()) {
        		String strtmp = matcher2.group();
        		name = strtmp != null ? name.concat(strtmp) : name;
//        		System.out.print(name + " | ");
        	}
//        	System.out.println("");
        	name = name.replaceAll("[\\d]:", "").trim().replaceAll(" +", " ");
        	lines.add(name);
        }
        // you may process the line here itself, as and when it is obtained
        
    }
	
//	public static void main(String[] args) throws Exception {
//		PDDocument document = PDDocument.load(new File("C:\\Working Stuff\\Client\\BRI Finance\\sample.pdf"),"brif");
//		//PDFTextStripper stripper = new PDFTextStripper();
//		PDFTextStripper stripper = new TestReadPDF();
//		//String text = stripper.getText(document);
//		
//		Writer dummy = new OutputStreamWriter(new ByteArrayOutputStream());
//		stripper.writeText(document, dummy);
//
//		
//		// print lines
//		int lineno = 0;
//		System.out.println(lines.size());
//		for(String line:lines){
//			lineno++;
//			System.out.println("(" + lineno +") " + line);
//			
//		}
//		//System.out.println("Text:" + text);
//		document.close();	
//
//	}
	
	public static void main(String[] args) throws Exception {
		PDDocument document = PDDocument.load(new File("C:\\Working Stuff\\Client\\BRI Finance\\sample.pdf"),"brif");
		//PDFTextStripper stripper = new PDFTextStripper();
		MyPDFTextParser stripper = new MyPDFTextParser();
		//String text = stripper.getText(document);

		Writer dummy = new OutputStreamWriter(new ByteArrayOutputStream());
		stripper.writeText(document, dummy);


		// print lines
		int lineno = 0;
		System.out.println(stripper.getLines().size());
		for(String line:stripper.getLines()){
			lineno++;
			System.out.println("(" + lineno +") " + line);

		}
		//System.out.println("Text:" + text);
		document.close();	
	}
}
