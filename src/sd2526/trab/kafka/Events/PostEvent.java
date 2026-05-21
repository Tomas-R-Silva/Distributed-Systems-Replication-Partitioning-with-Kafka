package sd2526.trab.kafka.Events;

import java.util.Set;

import sd2526.trab.api.Message;

public class PostEvent {
    
    private static final String TYPE = "POST";
    private long sid;
    private Message msg;
    public Set<String> localRecipients;
    
    


    public PostEvent(){}

   
    public static String getType() {
        return TYPE;
    }

    
    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

   
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
