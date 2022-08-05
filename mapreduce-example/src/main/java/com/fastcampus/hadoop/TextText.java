package com.fastcampus.hadoop;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TextText implements WritableComparable<TextText> {
    private Text first;
    private Text second;

    // 생성자 정의
    public TextText() {
        set(new Text(), new Text());
    }

    public TextText(String first, String second) {
        set(new Text(first), new Text(second));
    }

    public TextText(Text first, Text second) {
        set(first, second);
    }

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public Text getFirst() {
        return first;
    }

    public Text getSecond() {
        return second;
    }

    @Override
    public int compareTo(TextText o) {
        // 첫번 째 들어있는 first 값들을 기준으로 비교를 하고
        int cmp = first.compareTo(o.first);
        // 일치하지 않을 때에
        if (cmp != 0) {
            return cmp;
        }

        // 일치할땐 second 값들을 리턴
        return second.compareTo(o.second);
    }

    /** Writable 인터페이스 밑에 있는 두개의 메소드를 재정의 */
    // 직렬화를 위한
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }

    // 직렬화된 데이터를 읽을 때
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }

    // Hashcode 재정의
    @Override
    public int hashCode() {
        // 해쉬코드 값을 재정의 할때는 소수를 많이 곱해줌
        return first.hashCode() * 163 + second.hashCode();
    }

    // equals 재정의
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TextText) {
            TextText tt = (TextText) obj;
            return first.equals(tt.first) && second.equals(tt.second);
        }

        return false;
    }

    // toString 재정의
    @Override
    public String toString() {
        return first.toString() + ", " + second.toString();
    }
}
