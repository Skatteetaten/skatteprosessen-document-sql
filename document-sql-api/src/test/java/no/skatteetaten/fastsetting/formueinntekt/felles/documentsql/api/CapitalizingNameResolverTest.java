package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;

import org.junit.Test;

public class CapitalizingNameResolverTest {

    private static final int MAXIMUM = 16;

    @Test
    public void capitalizes_name() {
        assertThat(new CapitalizingNameResolver(MAXIMUM).resolve("fooBar", "quxBaz")).isEqualTo("FOO_BAR_QUX_BAZ");
    }

    @Test
    public void abbreviates_element() {
        assertThat(new CapitalizingNameResolver(MAXIMUM).resolve("fooBarFoo", "quxBaz")).isEqualTo("FBF_QUX_BAZ");
    }

    @Test
    public void abbreviates_multiple_elements() {
        assertThat(new CapitalizingNameResolver(MAXIMUM).resolve("fooBarFoo", "quxBazQux", "quxBaz")).isEqualTo("FBF_QBQ_QUX_BAZ");
    }

    @Test
    public void crops_element_after_initial() {
        assertThat(new CapitalizingNameResolver(MAXIMUM).resolve("fooBar", "barBaz", "quxBaz")).isEqualTo("FOO_BAR_QUX_BAZ");
    }

    @Test
    public void abreviates_element_after_initial() {
        assertThat(new CapitalizingNameResolver(MAXIMUM).resolve("fooBar", "X", "quxBaz")).isEqualTo("FB_X_QUX_BAZ");
    }

    @Test
    public void does_not_tolerate_duplication() {
        assertThat(new CapitalizingNameResolver(MAXIMUM).resolve(Arrays.asList("fooBar", "quxBaz"), "FOO_BAR_QUX_BAZ"::equals))
            .isNotEqualTo("FOO_BAR_QUX_BAZ")
            .startsWith("FOO_BAR_QUX_");
    }

    @Test
    public void can_abbreviate_camel_case() {
        assertThat(CapitalizingNameResolver.toAbbreviation("fooBar")).isEqualTo("FB");
    }

    @Test
    public void can_abbreviate_upper_case() {
        assertThat(CapitalizingNameResolver.toAbbreviation("XX__YY")).isEqualTo("XY");
    }

    @Test
    public void retains_digits() {
        assertThat(CapitalizingNameResolver.toAbbreviation("fooBar123")).isEqualTo("FB123");
    }
}