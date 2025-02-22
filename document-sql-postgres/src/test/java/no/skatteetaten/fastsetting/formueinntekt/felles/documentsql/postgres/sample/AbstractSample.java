package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.postgres.sample;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import jakarta.xml.bind.annotation.XmlSeeAlso;

@XmlSeeAlso(SimpleSample.class)
@JsonSubTypes(@JsonSubTypes.Type(SimpleSample.class))
public abstract class AbstractSample { }
