package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "sample")
public class NestedViewSample {

    private List<ViewSample> other;

    public List<ViewSample> getOther() {
        return other;
    }

    public void setOther(List<ViewSample> other) {
        this.other = other;
    }
}
