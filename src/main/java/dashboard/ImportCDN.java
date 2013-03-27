/*
 * Copyright (c) 2013 Dror. All rights reserved
 * <p/>
 * The software source code is proprietary and confidential information of Dror.
 * You may use the software source code solely under the terms and limitations of
 * the license agreement granted to you by Dror.
 */

package dashboard;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;

import java.net.URL;
import java.net.URLConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.Date;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import java.util.Properties;

// For date conversion to UNIX epoch format
import java.text.SimpleDateFormat;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.json.JSONObject;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.StringUtils;

// For the HTTP POST
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;

// Mixpanel
import com.mixpanel.mixpanelapi.ClientDelivery;
import com.mixpanel.mixpanelapi.MessageBuilder;
import com.mixpanel.mixpanelapi.MixpanelAPI;

// Amazon
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;

/**
 * Reads CDNs from Amazon CloudFront backet 
 * Import events to Mixpanel
 *
 */

public class ImportCDN
{
   static String TOKEN = "";
   static String API_KEY = "";
   static String bucketName = "";
   static String AWS_USER = "";
   static String AWS_PASS = "";
   static String DELETE_PROCESSED_LOGS = "";
   
    public static void main(String[] args)
 
    {
      int n = 0;
      String propertiesFileName = "";
      
      // First argument - number of events to import
      if (args.length > 0) {
        try {
           n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
           System.err.println("First argument must be an integer");
           System.exit(1);
        }
      } else { 
           System.err.println("Please specify number of events to import.");
           System.exit(1);
      }

      // Second argument - properties file name
      if (args.length > 1) 
        propertiesFileName = args[1];
      else 
        propertiesFileName = "gigaDashboard.properties";
      
      // Read Properties file
      Properties prop = new Properties(); 
      try {
           //load a properties file
    		   prop.load(new FileInputStream(propertiesFileName));
         
           // Another option - load default properties from the Jar
           //prop.load(ImportCDN.class.getResourceAsStream("/gigaDashboard.properties"));
 
           //get the property values
           TOKEN = prop.getProperty("MIXPANEL_GIGA_PROJECT_TOKEN");
           API_KEY = prop.getProperty("MIXPANEL_GIGA_API_KEY");
           bucketName = prop.getProperty("S3_BUCKET_NAME");
           AWS_USER = prop.getProperty("AWS_USER");
           AWS_PASS = prop.getProperty("AWS_PASS");
           DELETE_PROCESSED_LOGS = prop.getProperty("DELETE_PROCESSED_LOGS");
                   
           System.out.println("MIXPANEL PROJECT TOKEN = " + TOKEN);
       		System.out.println("MIXPANEL API KEY = " + API_KEY);
       		System.out.println("DELETE_PROCESSED_LOGS = " + DELETE_PROCESSED_LOGS);
    		   System.out.println("S3_BUCKET_NAME = " + prop.getProperty("S3_BUCKET_NAME"));
       		System.out.println("AWS_USER = " + prop.getProperty("AWS_USER"));
       		System.out.println("AWS_PASS = " + prop.getProperty("AWS_PASS"));
           System.out.println( "===================");
    	} catch (IOException ex) {
    		ex.printStackTrace();
        System.err.println("Can't find Propertie file - " + propertiesFileName);
        System.err.println("Second argument must be properties file name");
        System.exit(1);
      }

      try {
         System.out.println("\n>>> Starting to import " + n + " events... \n");
         readAmazonLogs(n);
      } 
      catch (Exception e) {
         e.printStackTrace();
      }   
    }    
    
   // ======================================  
   // Read Amazon CloudFront Logs (Zip files) 
   // 
   // ======================================
   public static int readAmazonLogs(int n) throws Exception {
        if (n < 1) return 0;  
        int eventsNumber = 0;
        String line = null;
        int begin = 0;
        int zips = 0;
        int deletedZips = 0;
        int mixpanelStatus = 0;
        
        // Log files Bucket
        AWSCredentials credentials = new BasicAWSCredentials(AWS_USER,AWS_PASS);
        AmazonS3Client s3Client = new AmazonS3Client(credentials);
        ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(bucketName);
        
        // Set MARKER - from which Log File to start reading
        // listObjectsRequest.setMarker("E2DXXJR0N8BXOK.2013-03-18-10.ICK6IvaY.gz");
        
        BufferedReader br = null;
        
        ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);
        ObjectListing nextObjectListing = objectListing;
        zips = 0;
        Boolean more = true;
        if (objectListing == null) more = false;
                
        while (more) {
        // Reads 1000 files
        for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
          // Handle  ZIP files        
          
          try { // Open and send to mixpanel events of one ZIP file  
            String key = objectSummary.getKey();
         
            S3Object object = s3Client.getObject(new GetObjectRequest(bucketName, key));
            // Extract ZIP and read Object to reader
            br = new BufferedReader(new InputStreamReader(new GZIPInputStream(object.getObjectContent())));
            zips++;      
            
            // Read the lines from the unzipped file, break it and send to Mixpanel
            while ((line = br.readLine()) != null) {
                if (line.startsWith("#")) continue;
                if (line.trim().equals("")) continue;
                String[] values = line.split("\\s");  
                
                String eventTime = values[0] + " " + values[1];
                String ip = values[4];
                String method = values[5];
                String fileName = values[7];
                String statusCode = values[8];
                String userAgent = values[10];
                String fName = fileName;
                
                if (fileName.contains("gigaspaces-")) {
                   begin = fileName.lastIndexOf("gigaspaces-") + 11;
                   fName = fileName.substring(begin, fileName.length()); 
                }

                eventsNumber++; 
                System.out.println(eventsNumber + ": " + eventTime + " " + ip );
                // Track the event in Mixpanel (using the POST import)
                mixpanelStatus = postCDNEventToMixpanel(ip, "Cloudfront CDN", eventTime, method,  fileName, fName, userAgent, statusCode);
      
            } // while on ZIP file lines
     
            if (mixpanelStatus == 1 & DELETE_PROCESSED_LOGS.equals("YES")) { 
                  // Delete the CDN log ZIP file
                  s3Client.deleteObject(bucketName, key);
                  System.out.println("============ Deleted Zip " + zips + " ============"); 
                  deletedZips++;
            } else
                  System.out.println("============ Zip " + zips + " (not deleted) ============"); 
         } catch (IOException e) {			
               e.printStackTrace();
               return eventsNumber;
		     } finally {
				     if (br != null) {
                br.close();
             }
   
             if (eventsNumber >= n) { 
                System.out.println("\n>>> " + eventsNumber + " events in " + zips + " Zip files. Deleted " + deletedZips + " Zip files.\n");
                return eventsNumber;
            }
         }
                
        } // for (continue to next ZIP file
        
        // If there are more ZIP files, read next batch of 1000
        if (objectListing.isTruncated()) {
            nextObjectListing = s3Client.listNextBatchOfObjects(objectListing);
            objectListing = nextObjectListing;
        } else 
            more = false; // no more files
        
       } // while next objectListing
        
       System.out.println("\n>>> " + eventsNumber + " events in " + zips + " Zip files. Deleted " + deletedZips + " Zip files.\n");
       return eventsNumber;
    }

   // =============================
   // Post CDN event to Mixpanel
   //
   // =============================
   public static int postCDNEventToMixpanel(String ip, String eventName, String eventTime, String method,
                                      String fileName, String fName, String userAgent, 
                                      String statusCode) throws IOException 
   {  
   try {
      SimpleDateFormat sdf  = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      Date date = sdf.parse( eventTime );
      long timeInSecSinceEpoch = date.getTime();
      if (timeInSecSinceEpoch > 0) timeInSecSinceEpoch = timeInSecSinceEpoch / 1000;   
      
      JSONObject obj1 = new JSONObject();
      obj1.put("distinct_id", ip);
      obj1.put("ip", ip);
      obj1.put("File path", fileName);
      obj1.put("File name", fName);
      obj1.put("User agent", userAgent);
      obj1.put("Status code", statusCode);
      obj1.put("Method", method);
      obj1.put("time", timeInSecSinceEpoch ); 
      obj1.put("token", TOKEN);
   
      JSONObject obj2 = new JSONObject();
      obj2.put("event", eventName);
      obj2.put("properties", obj1);

      String s2 = obj2.toString();
      String encodedJSON = Base64.encodeBase64String(StringUtils.getBytesUtf8(s2)); 
      
      return postRequest("http://api.mixpanel.com/import", "data", encodedJSON, "api_key", API_KEY );
     } catch (Exception e) {
         //throw new RuntimeException("Can't POST to Mixpanel.", e);
         e.printStackTrace();
         return 0;
     }
   }
  
   public static int postRequest(String url, String p1, String v1, String p2, String v2)
   {
    try {
     String contents = ""; 
     HttpClient client = new HttpClient();
     PostMethod method = new PostMethod( url );

	   // Configure the form parameters
	   method.addParameter( p1, v1 );
	   method.addParameter( p2, v2 );
    
     // Add more details in the POST response 
	   method.addParameter( "verbose", "1" );

	   // Execute the POST method 
     int statusCode = client.executeMethod( method );
     contents = method.getResponseBodyAsString();
     method.releaseConnection();
     if (statusCode != 200 || contents.charAt(11) != '1') {  // Post to Mixpanel Failed
        System.out.println("Mixpanel Post respone: " + Integer.toString(statusCode) + " - " + contents);
        return 0;
     }
     return 1;
    }
    catch( Exception e ) {
        e.printStackTrace();
        return 0;
    }
  }
}
