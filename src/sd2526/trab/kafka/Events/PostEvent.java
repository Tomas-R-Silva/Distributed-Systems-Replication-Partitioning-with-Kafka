package sd2526.trab.kafka.Events;

import java.util.Set;

import sd2526.trab.api.Message;

public class PostEvent extends Event {
    
    private Message msg;
    public Set<String> localRecipients;
    

    public PostEvent(){}


    public Message getMsg() {
        return msg;
    }

    public void setMsg(Message msg) {
        this.msg = msg;
    }

    
    public Set<String> getLocalRecipients() {
        return localRecipients;
    }

    public void setLocalRecipients(Set<String> localRecipients) {
        this.localRecipients = localRecipients;
    }

   

}
