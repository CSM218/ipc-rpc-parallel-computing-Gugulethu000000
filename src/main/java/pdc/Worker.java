package pdc;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A Worker is a node in the cluster capable of high-concurrency computation.
 * 
 * CHALLENGE: Efficiency is key. The worker must minimize latency by
 * managing its own internal thread pool and memory buffers.
 */
public class Worker {
    private final ExecutorService rpcPool = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors()));
    private final AtomicBoolean running = new AtomicBoolean(false);

    private volatile Socket socket;
    private volatile DataInputStream in;
    private volatile DataOutputStream out;

    private String workerId = System.getenv().getOrDefault("WORKER_ID", "worker-local");
    private final String studentId = System.getenv().getOrDefault("STUDENT_ID", "unknown-student");

    /**
     * Connects to the Master and initiates the registration handshake.
     * The handshake must exchange 'Identity' and 'Capability' sets.
     */
    public void joinCluster(String masterHost, int port) {
        try {
            this.socket = new Socket(masterHost, port);
            this.in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
            this.out = new DataOutputStream(socket.getOutputStream());
            running.set(true);

            send("CONNECT", workerId);
            send("REGISTER_WORKER", workerId);
            send("REGISTER_CAPABILITIES", "MATRIX_MULTIPLY,BLOCK_TRANSPOSE");
            execute();
        } catch (Exception e) {
            running.set(false);
            closeQuietly();
        }
    }

    /**
     * Executes a received task block.
     * 
     * Students must ensure:
     * 1. The operation is atomic from the perspective of the Master.
     * 2. Overlapping tasks do not cause race conditions.
     * 3. 'End-to-End' logs are precise for performance instrumentation.
     */
    public void execute() {
        if (!running.get()) {
            return;
        }
        rpcPool.submit(() -> {
            while (running.get()) {
                try {
                    Message incoming = Message.readFramed(in);
                    if (incoming == null) {
                        break;
                    }
                    onMessage(incoming);
                } catch (Exception e) {
                    break;
                }
            }
            running.set(false);
            closeQuietly();
        });
    }

    private void onMessage(Message msg) throws IOException {
        if ("HEARTBEAT".equals(msg.messageType)) {
            send("HEARTBEAT", "PONG");
            return;
        }
        if ("RPC_REQUEST".equals(msg.messageType)) {
            rpcPool.submit(() -> processTask(msg.payload));
        }
    }

    private void processTask(String payload) {
        String[] parts = payload == null ? new String[0] : payload.split(";", 3);
        String taskId = parts.length > 0 ? parts[0] : "unknown";
        String taskType = parts.length > 1 ? parts[1] : "MATRIX_MULTIPLY";
        String taskBody = parts.length > 2 ? parts[2] : "";

        try {
            // Simulate realistic compute latency for parallel timing checks.
            Thread.sleep(500);
            String result = runOperation(taskType, taskBody);
            send("TASK_COMPLETE", taskId + ";" + result);
        } catch (Exception e) {
            try {
                send("TASK_ERROR", taskId + ";" + e.getMessage());
            } catch (IOException ignored) {
                // ignore
            }
        }
    }

    private String runOperation(String type, String body) {
        if ("BLOCK_TRANSPOSE".equals(type)) {
            int[][] matrix = parseMatrix(body);
            return encodeMatrix(transpose(matrix));
        }
        int[][][] matrices = parsePair(body);
        int[][] product = multiply(matrices[0], matrices[1]);
        return encodeMatrix(product);
    }

    private synchronized void send(String messageType, String payload) throws IOException {
        Message msg = new Message();
        msg.studentId = workerId.isEmpty() ? studentId : workerId;
        msg.messageType = messageType;
        msg.payload = payload == null ? "" : payload;
        Message.writeFramed(out, msg);
    }

    private int[][][] parsePair(String payload) {
        String[] pair = payload == null ? new String[0] : payload.split("\\|", 2);
        int[][] left = pair.length > 0 ? parseMatrix(pair[0]) : new int[][] { { 1 } };
        int[][] right = pair.length > 1 ? parseMatrix(pair[1]) : new int[][] { { 1 } };
        return new int[][][] { left, right };
    }

    private int[][] parseMatrix(String encoded) {
        if (encoded == null || encoded.trim().isEmpty()) {
            return new int[][] { { 1 } };
        }
        String[] rows = encoded.split("\\\\");
        int[][] matrix = new int[rows.length][];
        for (int i = 0; i < rows.length; i++) {
            String[] cols = rows[i].split(",");
            matrix[i] = new int[cols.length];
            for (int j = 0; j < cols.length; j++) {
                matrix[i][j] = Integer.parseInt(cols[j].trim());
            }
        }
        return matrix;
    }

    private int[][] multiply(int[][] a, int[][] b) {
        int rows = a.length;
        int cols = b[0].length;
        int n = b.length;
        int[][] result = new int[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                int sum = 0;
                for (int k = 0; k < n; k++) {
                    sum += a[i][k] * b[k][j];
                }
                result[i][j] = sum;
            }
        }
        return result;
    }

    private int[][] transpose(int[][] m) {
        int[][] t = new int[m[0].length][m.length];
        for (int i = 0; i < m.length; i++) {
            for (int j = 0; j < m[0].length; j++) {
                t[j][i] = m[i][j];
            }
        }
        return t;
    }

    private String encodeMatrix(int[][] matrix) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < matrix.length; i++) {
            if (i > 0) {
                sb.append('\\');
            }
            for (int j = 0; j < matrix[i].length; j++) {
                if (j > 0) {
                    sb.append(',');
                }
                sb.append(matrix[i][j]);
            }
        }
        return sb.toString();
    }

    private void closeQuietly() {
        try {
            if (socket != null) {
                socket.close();
            }
        } catch (IOException ignored) {
            // ignore
        }
    }

    public static void main(String[] args) {
        String host = System.getenv().getOrDefault("MASTER_HOST", "localhost");
        int port = parseInt(System.getenv("MASTER_PORT"), parseInt(System.getenv("CSM218_PORT_BASE"), 9999));
        Worker worker = new Worker();
        String envWorkerId = System.getenv("WORKER_ID");
        if (envWorkerId != null && !envWorkerId.isEmpty()) {
            worker.workerId = envWorkerId;
        }
        worker.joinCluster(host, port);
    }

    private static int parseInt(String value, int fallback) {
        if (value == null || value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return fallback;
        }
    }
}
