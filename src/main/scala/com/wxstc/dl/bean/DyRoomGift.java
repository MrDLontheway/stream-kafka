package com.wxstc.dl.bean;

import java.io.Serializable;
import java.util.List;

public class DyRoomGift implements Serializable {
    public String rid;
    public Object gifts;

    public String getRid() {
        return rid;
    }

    public void setRid(String rid) {
        this.rid = rid;
    }

    public Object getGifts() {
        return gifts;
    }

    public void setGifts(Object gifts) {
        this.gifts = gifts;
    }
}
