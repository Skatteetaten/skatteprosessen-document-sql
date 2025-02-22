package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

@JacksonXmlRootElement(localName = "sample")
public class ViewSample {

    private List<SimpleSample> list;

    public List<SimpleSample> getList() {
        return list;
    }

    public void setList(List<SimpleSample> list) {
        this.list = list;
    }
}
