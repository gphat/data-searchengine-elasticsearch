package Data::SearchEngine::ElasticSearch::Results;
use Moose;

# ABSTRACT: Search Results

extends 'Data::SearchEngine::Results';

with (
    'Data::SearchEngine::Results::Faceted',
);

no Moose;
__PACKAGE__->meta->make_immutable;
1;