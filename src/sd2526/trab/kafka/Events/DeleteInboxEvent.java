package sd2526.trab.kafka.Events;

public class DeleteInboxEvent extends Event {
    private String name;
    private String mid;
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

    public void setPwd(String pwd){
        this.pwd = pwd;
    }

    public String getPwd(){
        return pwd;
    }
}

