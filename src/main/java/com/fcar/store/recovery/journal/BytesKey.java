package com.fcar.store.recovery.journal;

import java.io.Serializable;

/**
 * 由于byte[]作为map的key时，会造成每次的key都不一样，所以必须封装一下。 <br />
 * 该类就是封装了byte[]
 * <p>
 * Created by zhangyong on 2017/1/26.
 */
public class BytesKey implements Serializable {

    /**
     * serialVersionUID
     */
    private static final long serialVersionUID = -6296965387124592707L;

    private byte[] data;


    public BytesKey(final byte[] data) {
        this.data = data;
    }


    /**
     * @return the data
     */
    public byte[] getData() {
        return data;
    }


    /**
     * @param data the data to set
     */
    public void setData(final byte[] data) {
        this.data = data;
    }


    @Override
    public int hashCode() {
        int h = 0;
        if (null != this.data) {
            for (int i = 0; i < this.data.length; i++) {
                h = 31 * h + data[i++];
            }
        }
        return h;
    }

    @Override
    public boolean equals(final Object o) {
        if (null == o || !(o instanceof BytesKey)) {
            return false;
        }
        final BytesKey k = (BytesKey) o;
        if (null == k.getData() && null == this.getData()) {
            return true;
        }
        if (null == k.getData() || null == this.getData()) {
            return false;
        }
        if (k.getData().length != this.getData().length) {
            return false;
        }
        for (int i = 0; i < this.data.length; ++i) {
            if (this.data[i] != k.getData()[i]) {
                return false;
            }
        }
        return true;
    }

}
