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

 XXX

=head1 DESCRIPTION

 XXX
 
=begin :prelude

=head1 IMPLEMENTATION NOTES

=head2 Queries

ElasticSearch's query DSL is complex and a pain in the ass to standardize in
any useful way.  Thus, it is expected that if your L<Data::SearchEngine::Query>
object has B<any> C<query> set then it must also have a C<type>.

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

sub search {
    my ($self, $query) = @_;

    my $options;
    if($query->has_query) {
        die "Queries must have a type." unless $query->has_type;
        $options->{query} = { $query->type => $query->query };
    }

    $options->{index} = $query->index;
    # if($query->has_filters) {
    #     $options->{fq} = [];
    #     foreach my $filter (keys %{ $query->filters }) {
    #         push(@{ $options->{fq} }, $query->get_filter($filter));
    #     }
    # }

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
    # 
    # my $facets = $resp->facet_counts;
    # if(exists($facets->{facet_fields})) {
    #     foreach my $facet (keys %{ $facets->{facet_fields} }) {
    #         $result->set_facet($facet, $facets->{facet_fields}->{$facet});
    #     }
    # }
    # if(exists($facets->{facet_queries})) {
    #     foreach my $facet (keys %{ $facets->{facet_queries} }) {
    #         $result->set_facet($facet, $facets->{facet_queries}->{$facet});
    #     }
    # }
    # 
    # foreach my $doc ($resp->docs) {
    foreach my $doc (@{ $resp->{hits}->{hits} }) {
    # 
    #     my %values;
    #     foreach my $fn ($doc->field_names) {
    #         my @n_values = $doc->values_for($fn);
    #         if (scalar(@n_values) > 1) {
    #             @{$values{$fn}} = @n_values;
    #         } else {
    #             $values{$fn} = $n_values[0];
    #         }
    #     }
    # 
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
