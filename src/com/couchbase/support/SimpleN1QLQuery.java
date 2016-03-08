package com.couchbase.support;

// Brian Williams
// March 8, 2016
// Uses Couchbase SDK 2.2.3

// SDK references and handy urls:
//
// Release Notes:
// http://developer.couchbase.com/documentation/server/4.0/sdks/java-2.2/release-notes.html
//
// Download links:
// http://developer.couchbase.com/documentation/server/4.0/sdks/java-2.2/download-links.html
// 
// Direct Link:
// http://packages.couchbase.com/clients/java/2.2.3/Couchbase-Java-Client-2.2.3.zip


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlParams;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.consistency.ScanConsistency;

// Be sure to
// create primary index on `BUCKETNAME`;
// from cbq

public class SimpleN1QLQuery {

	public static void main(String[] args) {

		System.out.println("About to create cluster connection");
		Cluster cluster = CouchbaseCluster.create("10.111.110.101"); 

		String bucketName = "BUCKETNAME";

		System.out.println("About to open bucket");
		Bucket bucket = cluster.openBucket(bucketName);

		String query1 = "SELECT * FROM `" + bucketName + "`;";

		System.out.println("About to execute: " + query1);

		N1qlQueryResult qr = bucket.query(N1qlQuery.simple(query1)); 
		System.out.println("# RESULTS: " + qr.allRows().size());

		for (N1qlQueryRow row : qr.allRows()) { 
			System.out.println(row.value().toString()); 
		}

		System.out.println("----------------------------------");

		String key = "brian1";
		N1qlParams preparedQueryN1qlParams = N1qlParams.build().consistency(ScanConsistency.REQUEST_PLUS).adhoc(false); 
		String statement = String.format("SELECT `click` FROM `%s` USE KEYS $key WHERE `click` IS VALUED", bucket.name()); 

		System.out.println("About to execute: " + statement);

		JsonObject placeholderValues = JsonObject.create().put("key", key); 

		System.out.println("placeholderValues: " + placeholderValues);

		N1qlQuery query = N1qlQuery.parameterized(statement, placeholderValues, preparedQueryN1qlParams); 

		System.out.println("query: " + query);

		N1qlQueryResult queryResult = bucket.query(query);

		System.out.println("# RESULTS: " + queryResult.allRows().size());

		for (N1qlQueryRow row : queryResult.allRows()) { 
			System.out.println(row.value().toString()); 
		}

		cluster.disconnect();
	}

}
