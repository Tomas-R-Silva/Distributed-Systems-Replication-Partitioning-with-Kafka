package sd2526.trab.impl.zoho;

import sd2526.trab.api.Message;

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

        Message msg = new Message(account.mailboxAddress(), account.mailboxAddress(), "subject", "content with the email with the ourorg domain");
        Zoho.getInstance().postMessage(account.mailboxAddress(), msg);
        
    }
}
