package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class SyntheticNamespacePrefixResolverTest {

    @Test
    public void assigns_namespaces() {
        assertThat(new SyntheticNamespacePrefixResolver(false, false).apply(new LinkedHashSet<>(Arrays.asList(
            "foo", "bar"
        )))).containsExactlyInAnyOrderEntriesOf(Map.of(
            "foo", "ns1", "bar", "ns2"
        ));
    }

    @Test
    public void assigns_empty_namespace() {
        assertThat(new SyntheticNamespacePrefixResolver(false, false).apply(new LinkedHashSet<>(Arrays.asList(
            "foo", ""
        )))).containsExactlyInAnyOrderEntriesOf(Map.of(
            "foo", "ns1", "", "ns2"
        ));
    }

    @Test
    public void assigns_empty_namespace_to_empty_namespace() {
        assertThat(new SyntheticNamespacePrefixResolver(true, false).apply(new LinkedHashSet<>(Arrays.asList(
            "foo", ""
        )))).containsExactlyInAnyOrderEntriesOf(Map.of(
            "foo", "ns1", "", ""
        ));
    }

    @Test
    public void assigns_empty_namespace_to_first() {
        assertThat(new SyntheticNamespacePrefixResolver(false, true).apply(new LinkedHashSet<>(Arrays.asList(
            "foo", ""
        )))).containsExactlyInAnyOrderEntriesOf(Map.of(
            "foo", "", "", "ns1"
        ));
    }

    @Test
    public void assigns_empty_namespace_to_empty_not_first() {
        assertThat(new SyntheticNamespacePrefixResolver(true, true).apply(new LinkedHashSet<>(Arrays.asList(
            "foo", ""
        )))).containsExactlyInAnyOrderEntriesOf(Map.of(
            "foo", "ns1", "", ""
        ));
    }
}
