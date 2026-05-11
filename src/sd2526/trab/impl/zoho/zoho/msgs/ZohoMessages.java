package sd2526.trab.impl.zoho.zoho.msgs;

public record ZohoMessages(
    String fromAddress,
    String toAddress,
    String subject,
    String content
) {}
