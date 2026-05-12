package sd2526.trab.impl.zoho.zoho.msgs;

import java.util.List;
import java.util.Map;

public record ZohoQueryReply(
    ZohoStatus status, 
    List<ZohoQueryMessages> data) {

}
