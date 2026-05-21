package sd2526.trab.kafka.Events;

public class DeleteMessageEvent {

    private String mid;
    private long sid;

    public DeleteMessageEvent() {
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
}
