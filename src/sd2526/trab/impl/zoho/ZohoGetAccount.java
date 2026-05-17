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
        //Message msg = new Message("705", account.mailboxAddress(), account.mailboxAddress(), "subject", "content with the email with the ourorg domain 706");
        //Zoho.getInstance().postMessage(account.mailboxAddress(), msg);

        //Teste do SearchInbox()
        System.out.println(Zoho.getInstance().searchInbox("706"));
        
    }
}
