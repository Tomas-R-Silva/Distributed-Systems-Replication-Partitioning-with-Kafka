package sd2526.trab.kafka.Events;

public class Event {
    long sid;
    String type;

    public long getSid() { return sid; }
    public void setSid(long sid) { this.sid = sid; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
}
