package starrocks;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

@Data
public class StreamLoadResult {

    public static final String STATUS_SUCCESS = "Success";

    @JsonProperty("TxnId")
    public Long txnId;

    @JsonProperty("Label")
    public String label;

    @JsonProperty("Status")
    public String status;

    @JsonProperty("ExistingJobStatus")
    public String existingJobStatus;

    @JsonProperty("Message")
    public String message;

    @JsonProperty("NumberTotalRows")
    public Long numberTotalRows;

    @JsonProperty("NumberLoadedRows")
    public Long numberLoadedRows;

    @JsonProperty("NumberFilteredRows")
    public Long numberFilteredRows;

    @JsonProperty("NumberUnselectedRows")
    public Long numberUnselectedRows;

    @JsonProperty("LoadBytes")
    public Long loadBytes;

    @JsonProperty("LoadTimeMs")
    public Long loadTimeMs;

    @JsonProperty("ErrorURL")
    public List<String> errorURLs;
}
