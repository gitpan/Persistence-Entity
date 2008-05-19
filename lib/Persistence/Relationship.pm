package Persistence::Relationship;

use strict;
use warnings;

use vars qw($VERSION);
use vars qw(@EXPORT_OK %EXPORT_TAGS $VERSION);

use Abstract::Meta::Class ':all';
use base 'Exporter';
use Carp 'confess';

use constant LAZY   => 0;
use constant EAGER  => 1;

use constant NONE      => 0;
use constant ALL       => 1;
use constant ON_INSERT => 2;
use constant ON_UPDATE => 3;
use constant ON_DELETE => 4;


$VERSION = 0.01;

@EXPORT_OK = qw(LAZY EAGER NONE ALL ON_INSERT ON_UPDATE ON_DELETE);
%EXPORT_TAGS = (all => \@EXPORT_OK);

=head1 NAME

Persistence::Relationship - Object relationship,

=cut

=head1 SYNOPSIS

use Persistence::Relationship ':all';

=head1 DESCRIPTION

Represents a base class for object relationship.

=head1 EXPORT

one_to_many method by ':all' tag.

=head2 ATTRIBUTES

=over

=item name

Relationship name

=cut

has '$.name' => (required => 1);


=item attribute

=cut

has '$.attribute' => (required => 1);


=item fetch_method

LAZY, EAGER

=cut

has '$.fetch_method' => (default => LAZY);


=item cascade

NONE, ALL ON_UPDATE, ON_DELETE, ON_INSERT

=cut

has '$.cascade' => (default => NONE);


=back

=head2 METHODS

=over

=cut

{
    my %meta;

=item one_to_many

=cut


    sub add_relationship {
        my ($class, $package, $name) = (shift, shift, shift);
        my $relation = $class->new(@_, name => $name);
        my $relations = $meta{$package} ||= {};
        my $attribute = $relation->attribute;
        $attribute->associated_class
            or confess "associated class must be defined for attribute: " . $attribute->name;
        $relations->{$attribute->name} = $relation;

        $relation->install_fetch_interceptor($attribute)
            if ($relation->fetch_method eq LAZY);
        
        $relation;
    }


=item relationship

=cut

    sub relationship {
        my ($class, $package) = @_;
        $meta{$package}
    }


=item insertable_to_many_relations

Returns all to many relation where insert applies.

=cut

    sub insertable_to_many_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next if ref($relation) eq 'Persistence::Relationship::ToOne';
            my $cascade = $relation->cascade;
            next if($cascade ne ALL && $cascade ne ON_INSERT);
            push @result, $relation;
        }
        @result;
    }


=item insertable_to_one_relations

Returns all to one relation where insert applies.

=cut

    sub insertable_to_one_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next unless ref($relation) eq 'Persistence::Relationship::ToOne';
            my $cascade = $relation->cascade;
            next if($cascade ne ALL && $cascade ne ON_INSERT);
            push @result, $relation;
        }
        @result;
    }


=item updatable_to_many_relations

Returns all relation where insert applies.

=cut

    sub updatable_to_many_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next if ref($relation) eq 'Persistence::Relationship::ToOne';
            my $cascade = $relation->cascade;
            next if($cascade ne ALL && $cascade ne ON_UPDATE);
            push @result, $relation;
        }
        @result;
    }


=item updatable_to_one_relations

Returns all relation where insert applies.

=cut

    sub updatable_to_one_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next if ref($relation) ne 'Persistence::Relationship::ToOne';
            my $cascade = $relation->cascade;
            next if($cascade ne ALL && $cascade ne ON_UPDATE);
            push @result, $relation;
        }
        @result;
    }


=item deleteable_to_many_relations

Returns all to many relation where insert applies.

=cut

    sub deleteable_to_many_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next if ref($relation) eq 'Persistence::Relationship::ToOne';
            my $cascade = $relation->cascade;
            next if($cascade ne ALL && $cascade ne ON_DELETE);
            push @result, $relation;
        }
        @result;
    }


=item deleteable_to_one_relations

Returns all to one relation where insert applies.

=cut

    sub deleteable_to_one_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next if ref($relation) ne 'Persistence::Relationship::ToOne';
            my $cascade = $relation->cascade;
            next if($cascade ne ALL && $cascade ne ON_DELETE);
            push @result, $relation;
        }
        @result;
    }


=item eager_fetch_relations

=cut

    sub eager_fetch_relations {
        my ($class, $obj_class) = @_;
        my $relations = $meta{$obj_class} or return;
        my @result;
        foreach my $attribute_name (keys %$relations) {
            my $relation = $relations->{$attribute_name};
            next if $relation->fetch_method ne EAGER;
            push @result, $relation;
        }
        @result;
    }

}


=item install_fetch_interceptor

=cut

sub install_fetch_interceptor {
    my ($self, $attribute) = @_;
    my $type = $attribute->perl_type;
    my $attr_name = $attribute->name;
    my %pending_fetch;
    my $class_name = $attribute->class;
    $attribute->set_on_read(
        sub {
           my ($this, $attribute, $scope, $index) = @_;
           my $values = $attribute->get_value($this);
           my $entity_manager = Persistence::Entity::Manager->manager($self->persistence_conext($class_name));
           unless ($entity_manager->has_lazy_fetch_flag($this, $attr_name)) {
                unless ($pending_fetch{$this}) {
                     $pending_fetch{$self} = 1;
                     my $orm = $entity_manager->find_entity_mappings($class_name);
                     $self->deserialise_attribute($this, $entity_manager, $orm);
                     delete $pending_fetch{$self};
                     $values = $attribute->get_value($this);
                }
                $entity_manager->add_lazy_fetch_flag($this, $attr_name);
           }

           if ($scope eq 'accessor') {
                return $values;
           } else {
                return $type eq 'Hash' ? $values->{$index} : $values->[$index]
           }
        }
    )
}



{
=item set_persistence_conext

Sets persistence context

=cut

    my %persistence_context;    
    sub set_persistence_conext {
        my ($class, $obj_class, $context_name) = @_;
        return if exists $persistence_context{$obj_class};
        $persistence_context{$obj_class} = $context_name
    }


=item persistence_conext

Returns persistent context for passed in class

=cut

    sub persistence_conext {
        my ($class, $obj_class) = @_;
        $persistence_context{$obj_class};
    }

}


=item values

Returns relations values as array ref, takes object as parameter

=cut

sub values {
    my ($self, $object) = @_;
    my $attribute = $self->attribute;
    my $accessor = $attribute->accessor;
    my $values = $object->$accessor;
    ref($values) eq 'HASH' ? [values %$values] : $values;
}


1;

__END__

=back

=head1 SEE ALSO

L<Persistence::Entity>
L<Persistence::Relationship::OneToMany>
L<Persistence::Relationship::ManyToMany>

=head1 COPYRIGHT AND LICENSE

The Persistence::Relationship module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, adrian@webapp.strefa.pl

=cut

1;
