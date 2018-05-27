package tests;

import static utils.Log.Log;

import java.net.URI;
import java.util.logging.Level;

import javax.net.ssl.SSLContext;

import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import api.storage.Namenode;
import discovery.Discover;
import sys.storage.rest.RestNamenodeClient;
import utils.IP;


public class TestNameNodeClient {
	public static final String NAMENODE = "Namenode";

	public static final int NAMENODE_PORT = 7777;

	public static void main(String[] args) throws Exception {
		new RestNamenodeClient(new URI("https://127.0.1.1:7777"));
	}
}
