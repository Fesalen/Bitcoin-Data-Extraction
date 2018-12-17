package bithunter.extractor.redis;

import java.util.Map;

public class AddressRoll {
    private String address;
    private String firstOutputTxid;
    private String firstInputTxid;
    private long firstOutputHeight = Integer.MAX_VALUE;
    private long firstInputHeight = Integer.MAX_VALUE;
    private String latestTxid;
    private long latestHeight;

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getFirstOutputTxid() {
        return firstOutputTxid;
    }

    public void setFirstOutputTxid(String firstOutputTxid) {
        this.firstOutputTxid = firstOutputTxid;
    }

    public String getFirstInputTxid() {
        return firstInputTxid;
    }

    public void setFirstInputTxid(String firstInputTxid) {
        this.firstInputTxid = firstInputTxid;
    }

    public long getFirstOutputHeight() {
        return firstOutputHeight;
    }

    public void setFirstOutputHeight(long firstOutputHeight) {
        this.firstOutputHeight = firstOutputHeight;
    }

    public long getFirstInputHeight() {
        return firstInputHeight;
    }

    public void setFirstInputHeight(long firstInputHeight) {
        this.firstInputHeight = firstInputHeight;
    }

    public String getLatestTxid() {
        return latestTxid;
    }

    public void setLatestTxid(String latestTxid) {
        this.latestTxid = latestTxid;
    }

    public long getLatestHeight() {
        return latestHeight;
    }

    public void setLatestHeight(long latestHeight) {
        this.latestHeight = latestHeight;
    }

    public void update(BlockJO.AddressJO jo) {
        if(jo.getDirection() == BlockJO.Direction.Input) {
            if(firstInputHeight > jo.getHeight()) {
                firstInputHeight = jo.getHeight();
                firstInputTxid = jo.getRecentTxId();
            }
        } else if(jo.getDirection() == BlockJO.Direction.Output) {
            if(firstOutputHeight > jo.getHeight()){
                firstOutputHeight = jo.getHeight();
                firstOutputTxid = jo.getRecentTxId();
            }
        }

        latestTxid = jo.recentTxId;
    }
}
