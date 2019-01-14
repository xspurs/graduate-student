package com.brctl.hadoop.bean;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Stripes所用对象
 * @author duanxiaoxing
 * @created 2019/1/1
 */
public class StripesWordPair extends MapWritable {


    @Override
    public String toString() {
        String res = "";
        for (Entry<Writable, Writable> entry: this.entrySet()) {
            Text key = (Text) entry.getKey();
            DoubleWritable value = (DoubleWritable) entry.getValue();
            res += key.toString() + ":" + value.get() + ";";
        }
        return res;
    }

    public void putAll(StripesWordPair wordPair) {
        for (Entry<Writable, Writable> entry: wordPair.entrySet()) {
            Text wordPairKey = (Text) entry.getKey();
            DoubleWritable wordPairValue = (DoubleWritable) entry.getValue();
            if (this.containsKey(wordPairKey)) {
                double update = ((DoubleWritable) this.get(wordPairKey)).get() + wordPairValue.get();
                this.put(wordPairKey, new DoubleWritable(update));
            } else {
                this.put(wordPairKey, wordPairValue);
            }
        }

    }
}
