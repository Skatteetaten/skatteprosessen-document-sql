package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample;

import java.util.List;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;

@JacksonXmlRootElement(localName = "sample")
public class ViewWrapperSample {

    @XmlElementWrapper(name = "wrapper")
    @JacksonXmlElementWrapper(localName = "wrapper") // Jackson does not read JAXB annotations by default.
    private List<SimpleSample> list;

    public List<SimpleSample> getList() {
        return list;
    }

    public void setList(List<SimpleSample> list) {
        this.list = list;
    }
}
