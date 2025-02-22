package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "sample")
public class NestedSample {

    private SimpleSample outer;

    public void setOuter(SimpleSample outer) {
        this.outer = outer;
    }

    public SimpleSample getOuter() {
        return outer;
    }
}
