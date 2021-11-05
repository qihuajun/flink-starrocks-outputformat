package starrocks;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import com.socialtouch.martech.mbasedataprocess.util.JsonUtil;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

abstract class AbstractStarRocksOutputFormat<IT> extends RichOutputFormat<IT> {

    /** 默认批次大小 */
    static final int DEFAULT_FLUSH_MAX_SIZE = 5000;
    /** 序列化版本号 */
    private static final long serialVersionUID = 1L;
    /** 日志log */
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStarRocksOutputFormat.class);
    /** 数据库链接url */
    private final String url;
    /** 数据库用户名 */
    private final String username;
    /** 数据库密码 */
    private final String password;
    /** 数据库链接url */
    private final String database;
    /** 数据库用户名 */
    private final String table;
    /** 数据库密码 */
    private final String[] columns;
    /** 批次大小 */
    private final int batchInterval;
    /** 列分隔符 */
    protected final String columnSeperator = "\t";

    /** 行分隔符 */
    private final String lineSeperator = "\n";

    private List<String> rows = new ArrayList<>();

    /** 累加器记录失败数量 */
    protected LongCounter sendCounter = new LongCounter();

    /** 累加器记录失败数量 */
    protected LongCounter importedCounter = new LongCounter();

    protected CloseableHttpClient client;

    public AbstractStarRocksOutputFormat(
            String url,
            String username,
            String password,
            String database,
            String table,
            String[] columns,
            int batchInterval) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.table = table;
        this.columns = columns;
        this.batchInterval = batchInterval;
    }

    /**
     * Configures this output format. Since output formats are instantiated generically and hence
     * parameterless, this method is the place where the output formats set their basic fields based
     * on configuration values.
     *
     * <p>This method is always called first on a newly instantiated output format.
     *
     * @param parameters The configuration with all parameters.
     */
    @Override
    public void configure(Configuration parameters) {}

    /**
     * Opens a parallel instance of the output format to store the result of its parallel instance.
     *
     * <p>When this method is called, the output format it guaranteed to be configured.
     *
     * @param taskNumber The number of the parallel instance.
     * @param numTasks The number of parallel tasks.
     * @throws IOException Thrown, if the output could not be opened due to an I/O problem.
     */
    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        super.getRuntimeContext().addAccumulator("sendCounter", sendCounter);
        super.getRuntimeContext().addAccumulator("importedCounter", importedCounter);

        final HttpClientBuilder httpClientBuilder =
                HttpClients.custom()
                        .setRedirectStrategy(
                                new DefaultRedirectStrategy() {
                                    @Override
                                    protected boolean isRedirectable(String method) {
                                        return true;
                                    }
                                });

        client = httpClientBuilder.build();
    }

    /**
     * Adds a record to the output.
     *
     * <p>When this method is called, the output format it guaranteed to be opened.
     *
     * @param record The records to add to the output.
     * @throws IOException Thrown, if the records could not be added to to an I/O problem.
     */
    @Override
    public void writeRecord(IT record) throws IOException {
        String data = convertRecordToString(record);
        rows.add(data);
        if (rows.size() >= batchInterval) {
            sendData(rows);
            rows = new ArrayList<>();
        }
    }

    /**
     * 生成BasiAuth Header
     *
     * @param username
     * @param password
     * @return
     */
    private String basicAuthHeader(String username, String password) {
        final String tobeEncode = username + ":" + password;
        byte[] encoded = Base64.encodeBase64(tobeEncode.getBytes(StandardCharsets.UTF_8));
        return "Basic " + new String(encoded);
    }

    /**
     * 向Starrocks发送数据
     *
     * @param rows
     * @throws IOException
     */
    private void sendData(List<String> rows) throws IOException {
        sendCounter.add(rows.size());
        String content = String.join(lineSeperator, rows);

        final String loadUrl = String.format("http://%s/api/%s/%s/_stream_load", url, database, table);
        HttpPut put = new HttpPut(loadUrl);
        StringEntity entity = new StringEntity(content, "UTF-8");
        put.setHeader(HttpHeaders.EXPECT, "100-continue");
        put.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader(username, password));
        // the label header is optional, not necessary
        // use label header can ensure at most once semantics
        put.setHeader("label", UUID.randomUUID().toString());
        put.setHeader("columns", String.join(",", columns));
        put.setEntity(entity);

        int i = 0;
        boolean imported = false;
        while (i < 3) {
            i++;

            try (CloseableHttpResponse response = client.execute(put)) {
                final int statusCode = response.getStatusLine().getStatusCode();

                String loadResult = "";
                if (response.getEntity() != null) {
                    loadResult = EntityUtils.toString(response.getEntity());
                }

                // statusCode 200 just indicates that starrocks be service is ok, not stream load
                // you should see the output content to find whether stream load is success
                if (statusCode != 200 || loadResult.equals("")) {
                    LOG.warn(String.format("Stream load failed, statusCode=%s loadResulte=%s", statusCode, loadResult));
                    continue;
                }

                StreamLoadResult result = JsonUtil.string2Obj(loadResult, StreamLoadResult.class);
                if (result != null
                        && result.getStatus() != null
                        && result.getStatus().equals(StreamLoadResult.STATUS_SUCCESS)) {
                    importedCounter.add(result.getNumberLoadedRows());
                    imported = true;
                    break;
                } else {
                    LOG.warn(String.format("Stream load failed, load result=%s", loadResult));
                }

            } catch (IOException exception) {
                LOG.warn("starrocks load exception: " + exception.getMessage());
            }
        }

        if (!imported) {
            throw new IOException("starrocks load failed after 3 times");
        }
    }

    /**
     * 记录转化成字符串
     *
     * @param record
     * @return
     */
    protected abstract String convertRecordToString(IT record);

    /**
     * Method that marks the end of the life-cycle of parallel output instance. Should be used to
     * close channels and streams and release resources. After this method returns without an error,
     * the output is assumed to be correct.
     *
     * <p>When this method is called, the output format it guaranteed to be opened.
     *
     * @throws IOException Thrown, if the input could not be closed properly.
     */
    @Override
    public void close() throws IOException {
        if (!rows.isEmpty()) {
            sendData(rows);

            rows = new ArrayList<>();
        }
    }
}
