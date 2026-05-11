package sd2526.trab.impl.zoho;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.impl.zoho.zoho.ZohoServiceFactory;
import sd2526.trab.impl.zoho.zoho.ZohoTokenManager;
import sd2526.trab.impl.zoho.zoho.msgs.ZohoAccount;
import sd2526.trab.impl.zoho.zoho.msgs.ZohoAccountReply;
import sd2526.trab.impl.zoho.zoho.msgs.ZohoMessages;
import sd2526.trab.impl.utils.JSON;

import sd2526.trab.api.Message;

public class Zoho {
	static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

	static final String CLIENT_ID     = "1000.H1M4XR9TFHBK6YAJPPMG40TTMH9GVU";
    static final String CLIENT_SECRET = "ac9ab95412414559d68f5983ba9e3920d29f752767";
    static final String REFRESH_TOKEN = "1000.ffa375df0ea43ac47605b044a21a5694.c009985b534ac1beee2c82ffbf5a27cf";

	private static final String ACCOUNTS = "/accounts";
    private static final String MESSAGES = "/messages";

    final OAuth20Service service;
    final ZohoTokenManager tokenManager;

    static Zoho instance;
    
    private Zoho() {
    	service = ZohoServiceFactory.buildService(CLIENT_ID, CLIENT_SECRET);
        tokenManager = new ZohoTokenManager(service, REFRESH_TOKEN);
    }
 
    synchronized public static Zoho getInstance() {
    	if( instance == null )
    		instance = new Zoho();
    	return instance;
    }

    public ZohoAccount getAccount() throws Exception {
        var accessToken = new OAuth2AccessToken( tokenManager.getValidAccessToken() );

        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS);
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
        	if( response.isSuccessful() ) {
        		var body = response.getBody();
        		var data = JSON.decode(body, ZohoAccountReply.class).data();
        		if (data == null || data.isEmpty()) return null;
        		return data.get(0);
        	}
        	else {
        		System.err.println( response.getCode() + "/" + response.getBody() );
        		return null;
        	}
        }
    }

    public void postMessage(String address, Message msg) throws Exception {
        var accessToken = new OAuth2AccessToken( tokenManager.getValidAccessToken() );

        OAuthRequest request = new OAuthRequest(Verb.POST, MAIL_API_BASE + ACCOUNTS + "/" + getAccount().accountId() + MESSAGES);
        ZohoMessages zohoMessages = new ZohoMessages(msg.getSender(), address, msg.getSubject(), msg.getContents());
        request.setPayload(JSON.encode(zohoMessages));
        request.addHeader("Content-Type", "application/json");
        service.signRequest(accessToken, request);

        System.out.println(request);
        System.out.println(zohoMessages);
        try (Response response = service.execute(request)) {
        	if( response.isSuccessful() ) {
        		var body = response.getBody();
          	    //var data = JSON.decode(body, ZohoAccountReply.class).data();
        		
        		
        	}
        	else {
        		System.err.println( response.getCode() + "/" + response.getBody() );
        	}
        }
        
    }
    
    
}