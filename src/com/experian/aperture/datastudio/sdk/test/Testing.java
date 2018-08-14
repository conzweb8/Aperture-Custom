package com.experian.aperture.datastudio.sdk.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

public class Testing {

	public static void main(String[] args) {
       	HttpClient httpClient = new DefaultHttpClient();
    	HttpPost httpPost = new HttpPost("http://35.192.129.194:5000/classify");
    	// Request parameters and other properties.
    	List<NameValuePair> params = new ArrayList<NameValuePair>();
    	params.add(new BasicNameValuePair("text", "Tintin cakep banget nih"));
    	try {
    		httpPost.addHeader("content-type", "application/json");
    		StringEntity paramsTxt = new StringEntity("{\"text\":\"read a book\"}") ;
    	    httpPost.setEntity(paramsTxt);
    	} catch (UnsupportedEncodingException e) {
    	    // writing error to Log
    	    e.printStackTrace();
    	}
    	/*
    	 * Execute the HTTP Request
    	 */
    	try {
    	    HttpResponse response = httpClient.execute(httpPost);
    	    HttpEntity respEntity = response.getEntity();

    	    if (respEntity != null) {
    	        // EntityUtils to get the response content
    	        String content =  EntityUtils.toString(respEntity);
    	        System.out.println(content);
    	        JSONObject obj = new JSONObject(content);
    	        System.out.println("Response : " +  obj.get("result"));
    	    }
    	} catch (ClientProtocolException e) {
    	    // writing exception to log
    	    e.printStackTrace();
    	} catch (IOException e) {
    	    // writing exception to log
    	    e.printStackTrace();
    	} catch (Exception ex) {
    		ex.printStackTrace();
    	}
	}
}
