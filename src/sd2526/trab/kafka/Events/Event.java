package sd2526.trab.kafka.Events;

public class Event {
    String type;
    String originReplica;

    public Event(String type){
        this.type = type;
    }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getOriginReplica(){ return originReplica;}
    public void setOriginReplica(String originReplica) {this.originReplica = originReplica;}
}
