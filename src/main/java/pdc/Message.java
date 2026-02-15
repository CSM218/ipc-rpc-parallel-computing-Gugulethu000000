package pdc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Message represents the communication unit in the CSM218 protocol.
 * 
 * Requirement: You must implement a custom WIRE FORMAT.
 * DO NOT use JSON, XML, or standard Java Serialization.
 * Use a format that is efficient for the parallel distribution of matrix
 * blocks.
 */
public class Message {
    public static final String MAGIC = "CSM218";
    public static final int VERSION = 1;

    public String magic;
    public int version;
    public String messageType;
    public String studentId;
    public long timestamp;
    public String payload;

    public Message() {
        this.magic = MAGIC;
        this.version = VERSION;
        this.messageType = "";
        this.studentId = System.getenv().getOrDefault("STUDENT_ID", "unknown-student");
        this.timestamp = System.currentTimeMillis();
        this.payload = "";
    }

    /**
     * Converts the message to a byte stream for network transmission.
     * Students must implement their own framing (e.g., length-prefixing).
     */
    public byte[] pack() {
        validate();
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(baos);
            writeString(out, magic);
            out.writeInt(version);
            writeString(out, messageType);
            writeString(out, studentId);
            out.writeLong(timestamp);
            writeString(out, payload);
            out.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to pack message", e);
        }
    }

    /**
     * Reconstructs a Message from a byte stream.
     */
    public static Message unpack(byte[] data) {
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }

        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));
            Message m = new Message();
            m.magic = readString(in);
            m.version = in.readInt();
            m.messageType = readString(in);
            m.studentId = readString(in);
            m.timestamp = in.readLong();
            m.payload = readString(in);
            m.validate();
            return m;
        } catch (IOException e) {
            throw new IllegalArgumentException("Invalid wire payload", e);
        }
    }

    public void validate() {
        if (!MAGIC.equals(magic)) {
            throw new IllegalArgumentException("Invalid magic: " + magic);
        }
        if (version != VERSION) {
            throw new IllegalArgumentException("Invalid version: " + version);
        }
        if (messageType == null || messageType.isEmpty()) {
            throw new IllegalArgumentException("messageType is required");
        }
        if (studentId == null || studentId.isEmpty()) {
            throw new IllegalArgumentException("studentId is required");
        }
        if (payload == null) {
            payload = "";
        }
    }

    public static void writeFramed(DataOutputStream out, Message msg) throws IOException {
        byte[] body = msg.pack();
        out.writeInt(body.length);
        out.write(body);
        out.flush();
    }

    public static Message readFramed(InputStream in) throws IOException {
        DataInputStream dataIn = (in instanceof DataInputStream) ? (DataInputStream) in : new DataInputStream(in);
        int length;
        try {
            length = dataIn.readInt();
        } catch (EOFException eof) {
            return null;
        }
        if (length <= 0 || length > 10_000_000) {
            throw new IOException("Invalid frame length: " + length);
        }
        byte[] body = new byte[length];
        dataIn.readFully(body);
        return unpack(body);
    }

    public String toJson() {
        return "{"
                + "\"magic\":\"" + escapeJson(magic) + "\","
                + "\"version\":" + version + ","
                + "\"messageType\":\"" + escapeJson(messageType) + "\","
                + "\"studentId\":\"" + escapeJson(studentId) + "\","
                + "\"timestamp\":" + timestamp + ","
                + "\"payload\":\"" + escapeJson(payload) + "\""
                + "}";
    }

    public static Message parse(String json) {
        if (json == null) {
            throw new IllegalArgumentException("json cannot be null");
        }
        String trimmed = json.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("json cannot be empty");
        }
        Map<String, String> map = parseFlatJsonObject(trimmed);
        Message m = new Message();
        if (map.containsKey("magic")) {
            m.magic = map.get("magic");
        }
        if (map.containsKey("version")) {
            m.version = Integer.parseInt(map.get("version"));
        }
        m.messageType = map.getOrDefault("messageType", m.messageType);
        m.studentId = map.getOrDefault("studentId", m.studentId);
        if (map.containsKey("timestamp")) {
            m.timestamp = Long.parseLong(map.get("timestamp"));
        }
        m.payload = map.getOrDefault("payload", m.payload);
        m.validate();
        return m;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > 10_000_000) {
            throw new IOException("Invalid string length: " + length);
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String escapeJson(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\') {
                sb.append("\\\\");
            } else if (c == '"') {
                sb.append("\\\"");
            } else if (c == '\n') {
                sb.append("\\n");
            } else if (c == '\r') {
                sb.append("\\r");
            } else if (c == '\t') {
                sb.append("\\t");
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static Map<String, String> parseFlatJsonObject(String json) {
        Map<String, String> out = new HashMap<>();
        String body = json;
        if (body.startsWith("{")) {
            body = body.substring(1);
        }
        if (body.endsWith("}")) {
            body = body.substring(0, body.length() - 1);
        }

        int idx = 0;
        while (idx < body.length()) {
            idx = skipWhitespace(body, idx);
            if (idx >= body.length()) {
                break;
            }
            if (body.charAt(idx) == ',') {
                idx++;
                continue;
            }
            if (body.charAt(idx) != '"') {
                break;
            }
            int keyEnd = findQuote(body, idx + 1);
            String key = unescapeJson(body.substring(idx + 1, keyEnd));
            idx = keyEnd + 1;
            idx = skipWhitespace(body, idx);
            if (idx < body.length() && body.charAt(idx) == ':') {
                idx++;
            }
            idx = skipWhitespace(body, idx);

            String value;
            if (idx < body.length() && body.charAt(idx) == '"') {
                int valueEnd = findQuote(body, idx + 1);
                value = unescapeJson(body.substring(idx + 1, valueEnd));
                idx = valueEnd + 1;
            } else {
                int nextComma = findComma(body, idx);
                value = body.substring(idx, nextComma).trim();
                idx = nextComma;
            }
            out.put(key, value);
        }
        return out;
    }

    private static int skipWhitespace(String s, int start) {
        int i = start;
        while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
            i++;
        }
        return i;
    }

    private static int findQuote(String s, int start) {
        int i = start;
        while (i < s.length()) {
            if (s.charAt(i) == '"' && s.charAt(i - 1) != '\\') {
                return i;
            }
            i++;
        }
        return s.length();
    }

    private static int findComma(String s, int start) {
        int i = start;
        while (i < s.length() && s.charAt(i) != ',') {
            i++;
        }
        return i;
    }

    private static String unescapeJson(String value) {
        StringBuilder sb = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '\\' && i + 1 < value.length()) {
                char n = value.charAt(i + 1);
                if (n == '\\' || n == '"') {
                    sb.append(n);
                    i++;
                    continue;
                }
                if (n == 'n') {
                    sb.append('\n');
                    i++;
                    continue;
                }
                if (n == 'r') {
                    sb.append('\r');
                    i++;
                    continue;
                }
                if (n == 't') {
                    sb.append('\t');
                    i++;
                    continue;
                }
            }
            sb.append(c);
        }
        return sb.toString();
    }
}
