package sd2526.trab.kafka.Events;

import java.util.List;

import sd2526.trab.api.Message;

public class PostEvent extends Event {
    
    private Message msg;
    public List<String> localRecipients;
    private String mid;
    //desitnatários remotos e locais, os que existem e não existem
    

    public PostEvent(){
        super("POST");
    }


    public Message getMsg() {
        return msg;
    }

    public String getMid(){
        return mid;
    }

    public void setMid(String mid){
        this.mid=mid;
    }

    public void setMsg(Message msg) {
        this.msg = msg;
    }

    
    public List<String> getLocalRecipients() {
        return localRecipients != null ? localRecipients : List.of();
    }

    public void setLocalRecipients(List<String> localRecipients) {
        this.localRecipients = localRecipients;
    }

   

}
