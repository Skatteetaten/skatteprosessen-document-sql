package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "sample")
public class NumericSample {

    private int val;

    public int getVal() {
        return val;
    }

    public void setVal(int val) {
        this.val = val;
    }
}
