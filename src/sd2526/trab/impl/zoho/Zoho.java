package sd2526.trab.impl.zoho;

import java.util.LinkedList;
import java.util.List;

import com.github.scribejava.core.model.OAuth2AccessToken;
import com.github.scribejava.core.model.OAuthRequest;
import com.github.scribejava.core.model.Response;
import com.github.scribejava.core.model.Verb;
import com.github.scribejava.core.oauth.OAuth20Service;

import sd2526.trab.impl.zoho.zoho.*;
import sd2526.trab.impl.zoho.zoho.msgs.*;
import sd2526.trab.impl.utils.JSON;

import sd2526.trab.api.Message;

public class Zoho {
	static final String MAIL_API_BASE = "https://mail.zoho.eu/api";

	static final String CLIENT_ID     = "1000.H1M4XR9TFHBK6YAJPPMG40TTMH9GVU";
    static final String CLIENT_SECRET = "ac9ab95412414559d68f5983ba9e3920d29f752767";
    static final String REFRESH_TOKEN = "1000.ffa375df0ea43ac47605b044a21a5694.c009985b534ac1beee2c82ffbf5a27cf";

	private static final String ACCOUNTS = "/accounts";
    private static final String MESSAGES = "/messages";
    private static final String SEARCH = "/search";
    private static final String FOLDERS = "/folders";
    private static final String VIEW = "/view";

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

        //Path
        OAuthRequest request = new OAuthRequest(Verb.POST, MAIL_API_BASE + ACCOUNTS + "/" + getAccount().accountId() + MESSAGES);
        //Header
        request.addHeader("Content-Type", "application/json");
        //Payload 
        String metadata = msg.getId() + "-" + msg.getSender() + "-" + msg.getCreationTime() + "-" + msg.getSubject() + "-" + msg.getContents() + "-" + msg.getDestination();
        ZohoMessages zohoMessages = new ZohoMessages(msg.getSender(), address, msg.getSubject(), metadata);
        request.setPayload(JSON.encode(zohoMessages));
        //OAuth Header
        service.signRequest(accessToken, request);

        try (Response response = service.execute(request)) {
        	if( response.isSuccessful() ) {
        		//var body = response.getBody();
        	}
        	else {
        		System.err.println( response.getCode() + "/" + response.getBody() );
        	}
        }
    }
    
    private List<ZohoQueryMessages> searchZoho(String query) throws Exception{
        var accessToken = new OAuth2AccessToken( tokenManager.getValidAccessToken() );

        //Path
        String formmattedQuery = "content:" + query;
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS + "/" + getAccount().accountId() + MESSAGES + SEARCH + "?searchKey=" + formmattedQuery);
        //Header
        request.addHeader("Content-Type", "application/json");
        //Payload
        //no payload
        //OAuth Header
        service.signRequest(accessToken, request);

        System.out.println(request);
        
        try (Response response = service.execute(request)) {
        	if( response.isSuccessful() ) {
        		var body = response.getBody();
          	    var data = JSON.decode(body, ZohoQueryReply.class).data();
                return data;
                //return null;
        	}
        	else {
        		System.err.println( response.getCode() + "/" + response.getBody() );
                return null;
        	}
        }
        
    }

    private List<ZohoQueryMessages> getEmailInFolder() throws Exception{
        var accessToken = new OAuth2AccessToken( tokenManager.getValidAccessToken() );

        //Path
        OAuthRequest request = new OAuthRequest(Verb.GET, MAIL_API_BASE + ACCOUNTS + "/" + getAccount().accountId() + MESSAGES + VIEW);
        //Header
        request.addHeader("Content-Type", "application/json");
        //Payload
        //no payload
        //OAuth Header
        service.signRequest(accessToken, request);

        System.out.println(request);
        
        try (Response response = service.execute(request)) {
        	if( response.isSuccessful() ) {
        		var body = response.getBody();
          	    var data = JSON.decode(body, ZohoQueryReply.class).data();
                return data;
                //return null;
        	}
        	else {
        		System.err.println( response.getCode() + "/" + response.getBody() );
                return null;
        	}
        }
    }

    public List<ZohoQueryMessages> getZohoMessage(String mid) throws Exception{
        List<ZohoQueryMessages> list = new LinkedList<>();
        List<ZohoQueryMessages> it = this.searchZoho(mid);
        for (ZohoQueryMessages zMsg : it) {
            String[] msg = zMsg.summary().split("-");
            if(msg[0].equals(mid)){
                ZohoQueryMessages folderIdAndMsgId = new ZohoQueryMessages(zMsg.fromAddress(), zMsg.folderId(), zMsg.messageId(), zMsg.sender(),zMsg.subject(),zMsg.summary(),zMsg.sentDateInGMT());
                list.add(folderIdAndMsgId);
            }
        }
        return list;
    }

    public List<String> getAllZohoMessages() throws Exception{
        List<ZohoQueryMessages> it = this.getEmailInFolder();
        List<String> list = new LinkedList<String>();
        for (ZohoQueryMessages zMsg : it) {
            String[] msg = zMsg.summary().split("-");
            if(!list.contains(msg[0])){
                list.add(msg[0]);
            }
        }
        return list;
    }

    public List<String> searchInbox(String query) throws Exception{
        List<ZohoQueryMessages> it = this.searchZoho(query);
        List<String> list = new LinkedList<String>();
        for (ZohoQueryMessages zMsg : it) {
            String[] msg = zMsg.summary().split("-");
            if(!list.contains(msg[0]) && (msg[3].contains(query) || msg[4].contains(query))){
                list.add(msg[0]);
            }
        }
        return list;
    }

    public void removeMessage(String mid) throws Exception{
        List<ZohoQueryMessages> msgZohoinfo = getZohoMessage(mid);

        for (ZohoQueryMessages zohoQueryMessages : msgZohoinfo) {
            String folderID = zohoQueryMessages.folderId();
            String msgID = zohoQueryMessages.messageId();

            var accessToken = new OAuth2AccessToken( tokenManager.getValidAccessToken() );

            //Path
            OAuthRequest request = new OAuthRequest(Verb.DELETE, MAIL_API_BASE + ACCOUNTS + "/" + getAccount().accountId() + FOLDERS + "/" + folderID + MESSAGES + "/" + msgID + "?expunge=true");
            //Header
            request.addHeader("Content-Type", "application/json");
            //Payload
            //no payload
            //OAuth Header
            service.signRequest(accessToken, request);

            try (Response response = service.execute(request)) {
                if( response.isSuccessful() ) {
                    //var body = response.getBody();
                }
                else {
                    System.err.println( response.getCode() + "/" + response.getBody() );
                }
            }   
        }

    }

    public void deleteUserInbox(String name)throws Exception{
        List<ZohoQueryMessages> it = this.searchZoho(name);
        List<String> list = new LinkedList<String>();
        for (ZohoQueryMessages zMsg : it) {
            String[] msg = zMsg.summary().split("-");
            if(!list.contains(msg[0]) && msg[5].contains(name)){
                list.add(msg[0]);
            }
        }

        for (String midToDelete : list) {
            this.removeMessage(midToDelete);
        }
    
    }

    
}