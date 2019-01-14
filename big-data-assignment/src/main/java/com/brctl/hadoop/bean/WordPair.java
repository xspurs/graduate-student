package com.brctl.hadoop.bean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Pairs所用对象
 * @author duanxiaoxing
 * @created 2018/12/30
 */
public class WordPair implements WritableComparable<WordPair> {

    private String wordA;
    private String wordB;

    public String getWordA() {
        return wordA;
    }

    public void setWordA(String wordA) {
        this.wordA = wordA;
    }

    public String getWordB() {
        return wordB;
    }

    public void setWordB(String wordB) {
        this.wordB = wordB;
    }

    @Override
    public int compareTo(WordPair o) {
        if (this.equals(o)) {
            return 0;
        } else {
            return (wordA + wordB).compareTo(o.getWordA() + o.getWordB());
        }
    }

    @Override
    public int hashCode() {
        return (wordA + wordB).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof WordPair)) {
            return false;
        } else {
            // 无序数对
            return (wordA.equals(((WordPair) o).getWordA())
                    && wordB.equals(((WordPair) o).getWordB()))
                    || (wordA.equals(((WordPair) o).getWordB())
                    && wordB.equals(((WordPair) o).getWordA()));
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(wordA);
        dataOutput.writeUTF(wordB);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wordA = dataInput.readUTF();
        wordB = dataInput.readUTF();
    }
}
