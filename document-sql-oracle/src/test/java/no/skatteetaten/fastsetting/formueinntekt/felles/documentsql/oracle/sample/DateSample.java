package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.oracle.sample;

import java.time.LocalDate;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "sample")
public class DateSample {

    private LocalDate val;

    public LocalDate getVal() {
        return val;
    }

    public void setVal(LocalDate val) {
        this.val = val;
    }
}
