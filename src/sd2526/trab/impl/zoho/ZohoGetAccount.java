package sd2526.trab.impl.zoho;

import java.util.*;

import sd2526.trab.api.Message;
import sd2526.trab.impl.utils.JSON;
import sd2526.trab.impl.zoho.zoho.msgs.ZohoMessages;
import sd2526.trab.impl.zoho.zoho.msgs.ZohoQueryMessages;

public class ZohoGetAccount {

    public static void main(String[] args) throws Exception {

        System.out.println("A correr");

        var account = Zoho.getInstance().getAccount();
        if (account != null)
            System.out.printf("Account ID: %s, displayName: %s\n", account.accountId(), account.displayName());
        else{
            System.err.println("Error...");
            return;   
        }

        //Teste do PostMessage()
        //Message msg = new Message("706", account.mailboxAddress(), account.mailboxAddress(), "subject", "content with the email with the ourorg domain");
        //Zoho.getInstance().postMessage(account.mailboxAddress(), msg);
        //Message msg = new Message("705", account.mailboxAddress(), account.mailboxAddress(), "subject", "content with the email with the ourorg domain 706");
        //Zoho.getInstance().postMessage(account.mailboxAddress(), msg);

        //Teste do GetInboxMessage()
        //String zMsg = Zoho.getInstance().getMessageZohoInfo("705").get(0).summary();
        //String[] splited = zMsg.split("-");
        //Message msg = new Message(splited[0],splited[1],Set.of(splited[5]),splited[3],splited[4]);
        //msg.setCreationTime(Long.parseLong(splited[2]));
        //System.out.println(msg);

        //Teste do SearchInbox()
        //System.out.println(Zoho.getInstance().searchInbox("706"));

        //Teste do RemoveMessage()
        //Zoho.getInstance().RemoveMessage("705");
        
    }
}
