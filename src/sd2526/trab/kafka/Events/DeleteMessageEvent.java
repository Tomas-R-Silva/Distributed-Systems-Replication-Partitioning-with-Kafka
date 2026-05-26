package sd2526.trab.kafka.Events;

public class DeleteMessageEvent extends Event {

    private String mid;

    public DeleteMessageEvent() {
        super("DELETE_MESSAGE");
    }

    public String getMid() {
        return mid;
    }

    public void setMid(String mid) {
        this.mid = mid;
    }

}
