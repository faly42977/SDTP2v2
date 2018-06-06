package utils.dropbox;



import org.pac4j.scribe.builder.api.DropboxApi20;

import com.github.scribejava.core.builder.ServiceBuilder;
import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import utils.JSON;

public class DropboxClient{
	private static final String CREATE_FILE_V2_URL = "https://content.dropboxapi.com/2/files/upload";
	private static final String DELETE_V2_URL = "https://api.dropboxapi.com/2/files/delete_v2";
	private static final String CREATE_DIR_V2_URL = "https://api.dropboxapi.com/2/files/create_folder";
	private static final String GET_FILE_V2_URL = "https://content.dropboxapi.com/2/files/download";
	protected static final String OCTETSTREAM_CONTENT_TYPE = "application/octet-stream";
	protected static final String JSON_CONTENT_TYPE = "application/json";

	private static final String DROPBOX_API_ARG = "Dropbox-API-Arg";

	static String apiKey = "3798wk62thvohz1";
	static String apiSecret ="6kjgm89e1j9vnn0";


	static OAuth20Service service = new ServiceBuilder().apiKey(apiKey).apiSecret(apiSecret).build(DropboxApi20.INSTANCE);
	static final String token = "_OX6R4XogdAAAAAAAAAAMMgVtppFSGpqdtec7SRkDYMBaEG99UapvbS2ISUqTksx";
	static OAuth2AccessToken  accessToken = new OAuth2AccessToken(token);

	public synchronized static boolean createFile(String path,byte[] contents) {
		OAuthRequest createFile = new OAuthRequest(Verb.POST, CREATE_FILE_V2_URL);
		createFile.addHeader("Content-Type", OCTETSTREAM_CONTENT_TYPE);
		createFile.addHeader(DROPBOX_API_ARG, JSON.encode(new CreateFile(path)));

		createFile.setPayload( contents );

		service.signRequest(accessToken, createFile);	
		Response r;
		try {
			r = service.execute(createFile);
			if (r.getCode() != 200) {
				//System.out.println(r.getBody());
				//System.out.println(r.getCode());
				throw new RuntimeException( r.getMessage() ) ;

			}
			//System.out.println("File successfully created..." + r.getCode());
			return true;
		} catch (Exception e) {
			e.printStackTrace();

		}
		return false;
	}

	public synchronized static boolean delete(String path) {
		OAuthRequest delete = new OAuthRequest(Verb.POST, DELETE_V2_URL);
		delete.addHeader("Content-Type", JSON_CONTENT_TYPE);
		//deleteFile.addHeader(DROPBOX_API_ARG, JSON.encode(new Delete(path)));
		//deleteFile.addHeader("Authorization","Bearer "+token);

		service.signRequest(accessToken, delete);	

		delete.setPayload( JSON.encode(new Data(path)) );
		Response r;
		try {
			r = service.execute(delete);
			if (r.getCode() != 200) {
				//System.out.println(r.getBody());
				//System.out.println(r.getCode());
				throw new RuntimeException( r.getMessage() ) ;

			}
			//System.out.println("File successfully deleted..." + r.getCode());
			return true;
		} catch (Exception e) {
			e.printStackTrace();

		}
		return false;
	}

	public synchronized static boolean createDir(String path){
		OAuthRequest createDir = new OAuthRequest(Verb.POST, CREATE_DIR_V2_URL);
		createDir.addHeader("Content-Type", JSON_CONTENT_TYPE);

		service.signRequest(accessToken, createDir);

		createDir.setPayload( JSON.encode(new Folder(path)) );
		Response r;
		try {
			r = service.execute(createDir);
			if (r.getCode() != 200) {
				//System.out.println(r.getBody());
				//System.out.println(r.getCode());
				throw new RuntimeException( r.getMessage() ) ;

			}
			//System.out.println("File successfully created..." + r.getCode());
			return true;
		} catch (Exception e) {
			e.printStackTrace();

		}
		return false;
	}

	public synchronized static byte[] getFile(String path) {
		OAuthRequest getFile = new OAuthRequest(Verb.POST, GET_FILE_V2_URL);
		getFile.addHeader("Content-Type", OCTETSTREAM_CONTENT_TYPE);
		getFile.addHeader(DROPBOX_API_ARG, JSON.encode(new Data(path)));
		service.signRequest(accessToken, getFile);
		Response r;
		try {
			r = service.execute(getFile);
			if (r.getCode() != 200) {
				//System.out.println(r.getBody());
				//System.out.println(r.getCode());
				throw new RuntimeException( r.getMessage() ) ;

			}
			//System.out.println("Got file..." + r.getCode());
			//System.out.println(r.getBody().toString());
			return r.getBody().getBytes();
		} catch (Exception e) {
			e.printStackTrace();

		}
		return null;
	}

	private static class Data {
		public String path;

		public Data(String path) {
			this.path=path;
		}
	}

	private static class Folder {
		public String path;
		boolean autorename;
		public Folder(String path) {
			this.path=path; 
			this.autorename=false;
		}
	}

	public static class CreateFile {
		final String path;
		final String mode;
		final boolean autorename;
		final boolean mute;

		public CreateFile(String path) {
			this.path = path;
			this.mode = "add";
			this.autorename = false;
			this.mute = false;
		}
	}
}

