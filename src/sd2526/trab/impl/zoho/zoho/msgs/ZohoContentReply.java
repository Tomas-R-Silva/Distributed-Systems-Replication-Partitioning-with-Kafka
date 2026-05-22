package sd2526.trab.impl.zoho.zoho.msgs;

import java.util.*;

public record ZohoContentReply(
    ZohoStatus status, 
    ZohoContent data
) {}
