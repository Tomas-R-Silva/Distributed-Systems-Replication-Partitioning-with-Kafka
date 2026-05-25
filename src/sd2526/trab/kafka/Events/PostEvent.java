package sd2526.trab.kafka.Events;

import java.util.List;

import sd2526.trab.api.Message;

public class PostEvent extends Event {
    
    private Message msg;
    public List<String> localRecipients;
    //desitnatários remotos e locais, os que existem e não existem
    

    public PostEvent(){}


    public Message getMsg() {
        return msg;
    }

    public void setMsg(Message msg) {
        this.msg = msg;
    }

    
    public List<String> getLocalRecipients() {
        return localRecipients;
    }

    public void setLocalRecipients(List<String> localRecipients) {
        this.localRecipients = localRecipients;
    }

   

}
