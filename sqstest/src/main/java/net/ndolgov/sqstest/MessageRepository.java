package net.ndolgov.sqstest;

/**
 * Remember all the messages currently being processed
 */
public interface MessageRepository {
    String get(String handle);

    String remove(String handle);

    String put(String handle, String queueUrl);
}
