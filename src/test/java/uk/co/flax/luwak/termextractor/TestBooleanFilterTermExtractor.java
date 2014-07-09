package uk.co.flax.luwak.termextractor;

import java.util.List;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.BooleanClause;
import static org.fest.assertions.api.Assertions.assertThat;
import org.junit.Before;
import org.junit.Test;
import uk.co.flax.luwak.termextractor.weights.CompoundRuleWeightor;

public class TestBooleanFilterTermExtractor {

    private FilterTermExtractor termExtractor;

    @Before
    public void setUp() {
         termExtractor = new FilterTermExtractor();
         termExtractor.addExtractors(
                 new BooleanFilterTermExtractor(CompoundRuleWeightor.DEFAULT_WEIGHTOR, termExtractor));
    }

    @Test
    public void allDisjunctionQueriesAreIncluded() {
        BooleanFilter booleanFilter = new BooleanFilter();
        booleanFilter.add(new TermFilter(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        booleanFilter.add(new TermFilter(new Term("field1", "term2")), BooleanClause.Occur.SHOULD);

        List<QueryTerm> terms = termExtractor.extract(booleanFilter);

        assertThat(terms).containsOnly(
                new QueryTerm("field1", "term1", QueryTerm.Type.EXACT),
                new QueryTerm("field1", "term2", QueryTerm.Type.EXACT));
    }

    @Test
    public void allNestedDisjunctionClausesAreIncluded() {
        BooleanFilter booleanFilter = new BooleanFilter();
        BooleanFilter nestedBooleanFilter = new BooleanFilter();
        nestedBooleanFilter.add(new TermFilter(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        nestedBooleanFilter.add(new TermFilter(new Term("field2", "term2")), BooleanClause.Occur.SHOULD);
        booleanFilter.add(new TermFilter(new Term("field3", "term3")), BooleanClause.Occur.SHOULD);
        booleanFilter.add(nestedBooleanFilter, BooleanClause.Occur.SHOULD);

        assertThat(termExtractor.extract(booleanFilter)).hasSize(3);
    }

    @Test
    public void allDisjunctionClausesOfAConjunctionAreExtracted() {
        BooleanFilter booleanFilter = new BooleanFilter();
        BooleanFilter nestedBooleanFilter = new BooleanFilter();
        nestedBooleanFilter.add(new TermFilter(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        nestedBooleanFilter.add(new TermFilter(new Term("field2", "term2")), BooleanClause.Occur.SHOULD);
        booleanFilter.add(new TermFilter(new Term("field3", "term3")), BooleanClause.Occur.SHOULD);
        booleanFilter.add(nestedBooleanFilter, BooleanClause.Occur.MUST);

        assertThat(termExtractor.extract(booleanFilter)).hasSize(2);
    }

    @Test
    public void conjunctionsOutweighDisjunctions() {
        BooleanFilter booleanFilter = new BooleanFilter();
        booleanFilter.add(new TermFilter(new Term("field1", "term1")), BooleanClause.Occur.SHOULD);
        booleanFilter.add(new TermFilter(new Term("field2", "term2")), BooleanClause.Occur.MUST);

        List<QueryTerm> terms = termExtractor.extract(booleanFilter);
        assertThat(terms).hasSize(1);
        assertThat(terms).containsOnly(new QueryTerm("field2", "term2", QueryTerm.Type.EXACT));
    }

}