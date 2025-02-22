package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle.sample;

import java.time.LocalDateTime;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "sample")
public class DateTimeSample {

    private LocalDateTime val;

    public LocalDateTime getVal() {
        return val;
    }

    public void setVal(LocalDateTime val) {
        this.val = val;
    }
}
