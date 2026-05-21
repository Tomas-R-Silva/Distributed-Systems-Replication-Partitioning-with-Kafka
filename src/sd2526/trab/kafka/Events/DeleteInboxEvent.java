package sd2526.trab.kafka.Events;

public class DeleteInboxEvent {
    private String name;
    private String mid;
    private long sid;
    private String pwd;

    public DeleteInboxEvent(){}


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

    public void setPwd(String pwd){
        this.pwd = pwd;
    }

    public String getPwd(){
        return pwd;
    }
}

