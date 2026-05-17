package sd2526.trab.impl.zoho.zoho.msgs;

public record ZohoQueryMessages(
    String fromAddress,
    String folderId,
    String messageId,
    String sender,
    String subject,
    String summary,
    String sentDateInGMT
) {
}
