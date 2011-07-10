package Data::SearchEngine::ElasticSearch;
use Moose;

# ABSTRACT: ElasticSearch support for Data::SearchEngine

use Clone qw(clone);
use ElasticSearch;
use Try::Tiny;

with (
    'Data::SearchEngine',
    'Data::SearchEngine::Modifiable'
);

use Data::SearchEngine::Paginator;
use Data::SearchEngine::ElasticSearch::Results;

=head1 SYNOPSIS

    use Data::SearchEngine::Query;
    use Data::SearchEngine::ElasticSearch;

    my $dse = Data::SearchEngine::ElasticSearch->new(
        url => $self->url
    );

    my $query = Data::SearchEngine::Query->new(
        index => $c->req->params->{type},
        page => $c->req->params->{page} || 1,
        count => $c->req->params->{count} || 10,
        order => { _score => { order => 'asc' } },
        type => 'query_string',
        facets => {
            etype => { terms => { field => 'etype' } },
            author_organization_literal => { terms => { field => 'author_organization_literal' } },
            author_literal => { terms => { field => 'author_literal' } },
            source_literal => { terms => { field => 'source_literal' } },
        }
    );

    my $results = $dse->search($query);

=head1 DESCRIPTION

Data::SearchEngine::ElasticSearch is a backend for Data::SearchEngine.  It
aims to generalize the features of L<ElasticSearch> so that application
authors are insulated from I<some> of the differences betwene search modules.

=begin :prelude

=head1 IMPLEMENTATION NOTES

=head2 Incomplete

ElasticSearch's query DSL is large and complex.  It is not well suited to
abstraction by a library like this one.  As such you will almost likely find
this abstraction lacking.  Expect it to improve as the author uses more of
ElasticSearch's features in applications.

=head2 Queries

It is expected that if your L<Data::SearchEngine::Query> object has B<any>
C<query> set then it must also have a C<type>.

The query is then passed on to L<ElasticSearch> thusly:

    $es->search(
        # ...
        query => { $query->type => $query->query }
        # ...
    );

So if you want to do a query_string query, you would set up your query like
this:

    my $query = Data::SearchEngine::Query->new(
        # ...
        type => 'query_string',
        query => { query => 'some query text' }
        # ...
    );

See the documents for
L<ElasticSearch's DLS|http://www.elasticsearch.org/guide/reference/query-dsl/>
for more details.

=head2 Indexing

ElasticSearch wants an C<index> and C<type> for each Item that is indexed. It
is expected that you will populate these values in the item thusly:

  my $item = Data::SearchEngine::Item->new(
    id => $something,
    values => {
        index => 'twitter',
        type => 'tweet',
        # and whatever else
    }
  );
  $dse->add($item);

=end :prelude

=cut

has '_es' => (
    is => 'ro',
    isa => 'ElasticSearch',
    lazy => 1,
    default => sub {
        my $self = shift;
        return ElasticSearch->new(
            servers => $self->servers,
            transport => $self->transport,
            trace_calls => $self->debug
        )
    }
);

=cut

=attr servers

The servers to which we'll be connecting.

=cut

has 'servers' => (
    is => 'ro',
    isa => 'Str|ArrayRef',
    default => '127.0.0.1:9200'
);

=attr transport

The transport to use.  Refer to L<ElasticSearch> for more information.

=cut

has 'transport' => (
    is => 'ro',
    isa => 'Str',
    default => 'http'
);

=method add ([ $items ])

Add items to the index.  Keep in mind that the L<Data::SearchEngine::Item>
should have values set for L<index> and L<type>.

=cut

sub add {
    my ($self, $items, $options) = @_;

    my @docs;
    foreach my $item (@{ $items }) {

        my $data = $item->values;
        push(@docs, {
            index => delete($data->{index}),
            type => delete($data->{type}),
            id => $item->id,
            data => $data
        })
    }
    $self->_es->bulk_index(\@docs);
}

=method present ($item)

Returns true if the L<Data::SearchEngine::Item> is present.  Uses the item's
C<id>.

=cut

sub present {
    my ($self, $item) = @_;
    
    my $data = $item->values;
    
    try {
        my $result = $self->_es->get(
            index => delete($data->{index}),
            type => delete($data->{type}),
            id => $item->id
        );
    } catch {
        # ElasticSearch throws an exception if the document isn't there.
        return 0;
    }
    
    return 1;
}

sub remove {
    die("not implemented");
}

=method remove_by_id ($item)

Remove the specified item from the index.  Uses the item's C<id>.

=cut

sub remove_by_id {
    my ($self, $item) = @_;

    my $data = $item->values;
    try {
        $self->_es->delete(
            index => delete($data->{index}),
            type => delete($data->{type}),
            id => $item->id
        );
    } catch {
        return 0;
    }
    
    return 1;
}

sub update {
    die("not implemented");
}

=method search ($query)

Search!

=cut

sub search {
    my ($self, $query) = @_;

    my $options;
    if($query->has_query) {
        die "Queries must have a type." unless $query->has_type;
        $options->{query} = { $query->type => $query->query };
    }

    $options->{index} = $query->index;

    if($query->has_filters) {
        $options->{filter} = {};
        foreach my $filter ($query->filter_names) {
            $options->{filter} = $query->get_filter($filter);
        }
    }

    if($query->has_facets) {
        $options->{facets} = $query->facets;
    }

    if($query->has_order) {
        $options->{sort} = $query->order;
    }

    $options->{from} = ($query->page - 1) * $query->count;
    $options->{size} = $query->count;

    my $start = time;
    my $resp = $self->_es->search($options);

    my $page = $query->page;
    my $count = $query->count;
    my $hit_count = $resp->{hits}->{total};
    my $max_page = $hit_count / $count;
    if($max_page != int($max_page)) {
        # If trying to calculate how many pages we _could_ have gives us a
        # non integer, add one to the page after inting it so we get the right
        # integer.
        $max_page = int($max_page) + 1;
    }
    if($page > $max_page) {
        $page = $max_page;
    }

    my $pager = Data::SearchEngine::Paginator->new(
        current_page => $page,
        entries_per_page => $count,
        total_entries => $hit_count
    );

    my $result = Data::SearchEngine::ElasticSearch::Results->new(
        query => $query,
        pager => $pager,
        elapsed => time - $start
    );

    if(exists($resp->{facets})) {
        foreach my $facet (keys %{ $resp->{facets} }) {
            my $href = $resp->{facets}->{$facet};
            if(exists($href->{terms})) {
                foreach my $term (@{ $href->{terms} }) {
                    $result->set_facet($facet, { count => $term->{count}, value => $term->{term} });
                }
            }
        }
    }
    foreach my $doc (@{ $resp->{hits}->{hits} }) {
        $result->add(Data::SearchEngine::Item->new(
            id      => $doc->{_id},
            values  => $doc->{_source},
        ));
    }

    return $result;
}

no Moose;
__PACKAGE__->meta->make_immutable;

1;
