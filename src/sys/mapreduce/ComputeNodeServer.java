package sys.mapreduce;

import static utils.Log.Log;

import java.net.InetSocketAddress;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.jws.WebService;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.xml.ws.Endpoint;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import api.mapreduce.ComputeNode;
import sys.mapreduce.centralized.CentralizedMapReduceEngine;
import sys.mapreduce.distributed.soap.v1.SoapMapReduceEngine;
import utils.JKS;
import utils.Props;

@WebService(serviceName = ComputeNode.NAME, targetNamespace = ComputeNode.NAMESPACE, endpointInterface = ComputeNode.INTERFACE)
public class ComputeNodeServer implements ComputeNode {
	
	private static final String SERVER_KEYSTORE = "/home/sd/server_blob.jks";
	private static final String SERVER_KEYSTORE_PWD = "123456";

	private static final String SERVER_TRUSTSTORE = "/home/sd/client.jks";
	private static final String SERVER_TRUSTSTORE_PWD = "123456";

	
	
	private static String PROPS_FILENAME = "/props/sd2018-tp2.props";
	private static String MAPREDUCE_ENGINE_PROP = "mapreduce-engine";

	private static final int COMPUTENODE_PORT = 6666;
	private static String baseURI = String.format("https://0.0.0.0:%d%s", COMPUTENODE_PORT, ComputeNode.PATH);

	final MapReduceEngine engine;

	protected ComputeNodeServer(MapReduceEngine engine) {
		this.engine = engine;
	}

	@Override
	public void mapReduce(String jobClassBlob, String inputPrefix, String outputPrefix, int outputPartitionsSize ) throws InvalidArgumentException {
		try {
			if( jobClassBlob == null || inputPrefix == null || outputPrefix == null || outputPartitionsSize <= 0 )
				throw new InvalidArgumentException("");

			System.err.println("Executing:" + jobClassBlob);
			engine.executeJob(jobClassBlob, inputPrefix, outputPrefix, outputPartitionsSize);
			System.err.println("Done:" + jobClassBlob);

		} catch (Exception x) {
			x.printStackTrace();
		}
	}


	@SuppressWarnings("restriction")
	public static void main(String[] args ) throws Exception {
		
		List<X509Certificate> trustedCertificates = new ArrayList<>();

		KeyStore ks = JKS.load(SERVER_KEYSTORE, SERVER_KEYSTORE_PWD);
		KeyStore ts = JKS.load(SERVER_TRUSTSTORE, SERVER_TRUSTSTORE_PWD);

		KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
		kmf.init(ks, SERVER_KEYSTORE_PWD.toCharArray());

		SSLContext ctx = SSLContext.getInstance("TLS");

		TrustManagerFactory tmf2 = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
		tmf2.init(ts);

		for (TrustManager tm : tmf2.getTrustManagers()) {
			if (tm instanceof X509TrustManager)
				trustedCertificates.addAll(Arrays.asList(((X509TrustManager) tm).getAcceptedIssuers()));
		}

		// Create a trust manager that does not validate certificate chains
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {

			@Override
			public void checkClientTrusted(X509Certificate[] certs, String authType) {
				System.err.println(certs[0].getSubjectX500Principal());
			}

			@Override
			public void checkServerTrusted(X509Certificate[] certs, String authType) {
			}

			@Override
			public X509Certificate[] getAcceptedIssuers() {
				return trustedCertificates.toArray(new X509Certificate[0]);
			}
		} };

		ctx.init(kmf.getKeyManagers(), trustAllCerts, null);

		HttpsServer httpsServer = HttpsServer.create( new InetSocketAddress("0.0.0.0", COMPUTENODE_PORT), -1);
		httpsServer.setHttpsConfigurator(new HttpsConfigurator( ctx ));
		httpsServer.setHttpsConfigurator (new HttpsConfigurator(ctx) {
		     @Override
			public void configure (HttpsParameters params) {
		    	 SSLParameters sslparams = ctx.getDefaultSSLParameters();
		    	 sslparams.setNeedClientAuth(true);
				params.setSSLParameters( sslparams );
		     }
		 });
		httpsServer.start();

		//Props.parseFile(PROPS_FILENAME);
		//String engineClass = Props.get(MAPREDUCE_ENGINE_PROP, SoapMapReduceEngine.class.toString());
		//Log.fine("MapReduceEngine: " + engineClass);
		
		
		
		MapReduceEngine engine = new CentralizedMapReduceEngine();//(MapReduceEngine)Class.forName( engineClass).newInstance(); 
		//Endpoint.publish(baseURI, new ComputeNodeServer( engine ) );
		Endpoint endpoint = Endpoint.create(new ComputeNodeServer( engine ) );
		endpoint.publish( httpsServer.createContext("/mapreduce") );
	}
}
