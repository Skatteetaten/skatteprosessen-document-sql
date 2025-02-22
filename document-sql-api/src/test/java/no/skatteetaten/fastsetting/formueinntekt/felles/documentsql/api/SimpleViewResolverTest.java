package no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api;

import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.sample.BranchListSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.sample.BranchSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.sample.TerminalListSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.sample.TerminalSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.sample.BranchRecursiveSample;
import no.skatteetaten.fastsetting.formueinntekt.felles.documentsql.api.sample.BranchRecursiveListSample;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SimpleViewResolverTest {

    @Test
    public void can_resolve_terminal_without_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(TerminalSample.class);
        assertThat(views).hasSize(1).containsKey(Collections.emptyList());
        assertThat(views.get(
            Collections.emptyList()
        )).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.singletonList(new PathElement("terminal")),
            String.class
        ));
    }

    @Test
    public void can_resolve_terminal_with_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(TerminalSample.class, "root");
        assertThat(views).hasSize(1).containsKey(Collections.singletonList(Collections.singletonList(new PathElement("root"))));
        assertThat(views.get(
            Collections.singletonList(Collections.singletonList(new PathElement("root")))
        )).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.singletonList(new PathElement("terminal")),
            String.class
        ));
    }

    @Test
    public void can_resolve_terminal_list_without_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(TerminalListSample.class);
        assertThat(views).hasSize(1).containsKey(Collections.singletonList(Collections.singletonList(new PathElement("terminals"))));
        assertThat(views.get(
            Collections.singletonList(Collections.singletonList(new PathElement("terminals")))
        )).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.emptyList(),
            String.class
        ));
    }

    @Test
    public void can_resolve_terminal_list_with_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(TerminalListSample.class, "root");
        assertThat(views).hasSize(1).containsKey(Arrays.asList(
            Collections.singletonList(new PathElement("root")),
            Collections.singletonList(new PathElement("terminals"))
        ));
        assertThat(views.get(Arrays.asList(
            Collections.singletonList(new PathElement("root")),
            Collections.singletonList(new PathElement("terminals"))
        ))).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.emptyList(),
            String.class
        ));
    }

    @Test
    public void can_resolve_branch_without_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(BranchSample.class);
        assertThat(views).hasSize(1).containsKey(Collections.emptyList());
        assertThat(views.get(
            Collections.emptyList()
        )).containsExactlyEntriesOf(Collections.singletonMap(
            Arrays.asList(new PathElement("branch"), new PathElement("terminal")),
            String.class
        ));
    }

    @Test
    public void can_resolve_branch_with_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(TerminalSample.class, "root");
        assertThat(views).hasSize(1).containsKey(Collections.singletonList(Collections.singletonList(new PathElement("root"))));
        assertThat(views.get(
            Collections.singletonList(Collections.singletonList(new PathElement("root")))
        )).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.singletonList(new PathElement("terminal")),
            String.class
        ));
    }

    @Test
    public void can_resolve_branch_list_without_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(BranchListSample.class);
        assertThat(views).hasSize(1).containsKey(Collections.singletonList(Collections.singletonList(new PathElement("branches"))));
        assertThat(views.get(Collections.singletonList(
            Collections.singletonList(new PathElement("branches"))
        ))).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.singletonList(new PathElement("terminal")),
            String.class
        ));
    }

    @Test
    public void can_resolve_branch_list_with_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver().resolve(BranchListSample.class, "root");
        assertThat(views).hasSize(1).containsKey(Arrays.asList(
            Collections.singletonList(new PathElement("root")),
            Collections.singletonList(new PathElement("branches"))
        ));
        assertThat(views.get(Arrays.asList(
            Collections.singletonList(new PathElement("root")),
            Collections.singletonList(new PathElement("branches"))
        ))).containsExactlyEntriesOf(Collections.singletonMap(
            Collections.singletonList(new PathElement("terminal")),
            String.class
        ));
    }

    @Test
    public void can_filter_property() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver(
            (path, property) -> property != String.class
        ).resolve(TerminalSample.class);
        assertThat(views).isEmpty();
    }

    @Test
    public void can_filter_branch() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver(
            (path, property) -> property != BranchListSample.class
        ).resolve(BranchListSample.class);
        assertThat(views).isEmpty();
    }

    @Test
    public void can_filter_root() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver(
            (path, property) -> property != TerminalSample.class
        ).resolve(TerminalSample.class);
        assertThat(views).isEmpty();
    }

    @Test(expected = IllegalArgumentException.class)
    public void can_detect_recursive_branch() {
        new SimpleViewResolver().resolve(BranchRecursiveSample.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void can_detect_recursive_branch_list() {
        new SimpleViewResolver().resolve(BranchRecursiveListSample.class);
    }

    @Test
    public void can_break_recursive_branch() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver(
            (path, property) -> path.isEmpty()
        ).resolve(BranchRecursiveSample.class);
        assertThat(views).isEmpty();
    }

    @Test
    public void can_break_recursive_branch_list() {
        Map<List<List<PathElement>>, Map<List<PathElement>, Class<?>>> views = new SimpleViewResolver(
            (path, property) -> path.isEmpty()
        ).resolve(BranchRecursiveListSample.class);
        assertThat(views).isEmpty();
    }
}
