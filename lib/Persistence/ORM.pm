package Persistence::ORM;

use strict;
use warnings;

use Abstract::Meta::Class ':all';

use Persistence::Relationship ':all';
use Persistence::Relationship::ToOne ':all';
use Persistence::Relationship::OneToMany ':all';
use Persistence::Relationship::ManyToMany ':all';

use vars qw(@EXPORT_OK %EXPORT_TAGS $VERSION);
use Carp 'confess';
use base 'Exporter';

$VERSION = 0.03;

@EXPORT_OK = qw(entity column trigger to_one one_to_many many_to_many LAZY EAGER NONE ALL ON_INSERT ON_UPDATE ON_DELETE);
%EXPORT_TAGS = (all => \@EXPORT_OK);

=head1 NAME

Persistence::ORM - Object-relational mapping.

=cut  

=head1 SYNOPSIS

    package Employee;

    use Abstract::Meta::Class ':all';
    use Persistence::ORM ':all';

    entity 'emp';
    column empno => has('$.no') ;
    column ename => has('$.name');


=head1 DESCRIPTION

Object-relational mapping module.

=head1 EXPORT

entity, column, trigger by 'all' tag

=head2 ATTRIBUTES

=over

=item class

class name

=cut

has '$.class' => (required => 1);


=item entity_name

entity name.

=cut

has '$.entity_name' => (required => 1);


=item columns

Columns map between database column and object attribute

=cut

has '%.columns' => (item_accessor => '_column');


=item trigger

Defines tigger that will execute on one of the following event
before_insert after_insert before_update after_update before_delete after_delete, on_fetch
Takes event name as first parameter, and callback as secound parameter.

    $entity_manager->trigger(before_insert => sub {
        my ($self) = @_;
        #do stuff
    });

=cut

{

    has '%.triggers' => (
        item_accessor => '_trigger',
        on_change => sub {
            my ($self, $attribute, $scope, $value, $key) = @_;
            if($scope eq 'mutator') {
                my $hash = $$value;
                foreach my $k (keys %$hash) {
                    $self->validate_trigger($k. $hash->{$k});
                }
            } else {
                $self->validate_trigger($key, $$value);
            }
            $self;
        },
    );
}


=back

=head2 METHODS

=over

=item entity

Creates a meta entity class.

=cut

sub entity {
    my ($name, $package) = @_;
    $package ||= caller();
    my $mapping_meta = __PACKAGE__->new(entity_name => $name, class => $package);
}


{
    my %meta;

=item column

Adds mapping between column name and related attribute.
column (column => has '$.attr1');

=cut

    sub column {
        my ($name, $attribute) = @_;
        my $attr_class = 'Abstract::Meta::Attribute';
        confess "secound parameter must be a ${attr_class}"
            unless ref($attribute) eq $attr_class;
        my $package = caller();
        my $mapping_meta = mapping_meta($package) or confess "no entity defined for class $package";
        $mapping_meta->_column($name, $attribute);
    }




=item initialise

=cut

    sub initialise {
        my ($self) = @_;
        $self->_check_attribute;
        $meta{$self->class} = $self;
    }

    
=item mapping_meta

Returns meta enity class.
Takes optionally package name as parameter.

=cut

    sub mapping_meta {
        my ($package) = @_;
        $package ||= caller();
        $meta{$package};
    }
}


=item _check_attribute

Checks if attributes mapped from column are unlbessed hash structure,
if they are then dynamicly adds them Abstract::Meta::Attribute to Abstract::Meta::Class

=cut

sub _check_attribute {
    my ($self) = @_;
    my $class = $self->class;
    my $columns = $self->columns;
    my $meta_class = Abstract::Meta::Class::meta_class($class);
    for my $column(keys %$columns) {
        my $meta_attribute = $columns->{$column};
        my $type = ref($meta_attribute);
        if($type eq 'HASH') {
            my $name = $meta_attribute->{name};
            $name = '$.' . $name unless ($name =~ m/[\$\@\%]\./);
            my %args = (storage_key => $meta_attribute->{name}, %$meta_attribute, name => $name, class => $class);
            my $attribute = $meta_class->attribute_class->new(%args);
            push @{$meta_class->attributes}, $attribute;
            $columns->{$column} = $attribute;
        }
    }
}


=item add_column

Adds columns.
Takes column name, attribute name;

=cut

sub add_column {
    my ($self, $column, $attribute_name) = @_;
    $self->_column($column, $self->attribute($attribute_name));
}


=item attribute

=cut

sub attribute {
    my ($self, $attribute_name) = @_;
    my $meta = Abstract::Meta::Class::meta_class($self->class)
        or confess "cant find meta class defintion (Abstract::Meta::Class) for " . $self->class;
    my $attribute = $meta->attribute($attribute_name)
        or confess "cant find attribute ${attribute_name} for class " . $self->class;
    $attribute;
}


=item deserialise

Deserialises resultset to object.

=cut

sub deserialise {
    my ($self, $args, $entity_manager) = @_;
    my $result = bless {
        $self->map_hash_to_attributes_storage($args)
    }, $self->class;
    $entity_manager->initialise_operation($self->entity_name, $result);
    $self->deserialise_eager_relation_attributes($result, $entity_manager);
    $entity_manager->complete_operation($self->entity_name);
    $self->run_event('on_fetch', $result);
    $result;
}


=item deserialise_eager_relation_attributes

=cut

sub deserialise_eager_relation_attributes {
    my ($self, $object, $entity_manager) = @_;
    Persistence::Relationship->set_persistence_conext(ref($object), $entity_manager->name);
    my @relations = Persistence::Relationship->eager_fetch_relations(ref($object));
    foreach my $relation (@relations) {
        $relation->deserialise_attribute($object, $entity_manager, $self);
    }
}



=item deserialise_lazy_relation_attributes

=cut

sub deserialise_lazy_relation_attributes {
    my ($self, $object, $entity_manager) = @_;
    Persistence::Relationship->set_persistence_conext(ref($object), $entity_manager->name);
    my @relations = Persistence::Relationship->lazy_fetch_relations(ref($object));
    foreach my $relation (@relations) {
        my $accessor = $relation->attribute->accessor;
        $object->$accessor;
    }
}

=item map_hash_to_attributes_storage

Transform source hash to the object hash.
Return hash where source key is mapped attribute storage key.
Takes hash ref of field values.

=cut

sub map_hash_to_attributes_storage {
    my ($self, $args) = @_;
    my $columns = $self->columns;
    my %result = map {
        my $attr = $columns->{$_};
        ($attr->storage_key, $args->{$_} )} keys %$columns;
    wantarray ? (%result) : \%result;
}


=item map_object_attributes_to_column_values

Maps keys on passed in hash to coresponding columns.
Takes object as parameter.

=cut

sub map_object_attributes_to_column_values {
    my ($self, $obj) = @_;
    my $columns = $self->columns;
    my %result = map {
        my $accessor = $columns->{$_}->accessor;
        ($_, $obj->$accessor )} keys %$columns;
    wantarray ? (%result) : \%result;
}


=item column_values

Transform objects attirubtes to column values

=cut

sub column_values {
    my ($self, $obj, @columns) = @_;
    my $columns = $self->columns;
    my %result = map {
        my $accessor = $columns->{$_}->accessor;
        ($_, $obj->$accessor )} @columns;
    wantarray ? (%result) : \%result;
}


=item serialise

Serialised retrives date to object.

=cut

sub serialise {
    my ($self, $args) = @_;
    my $columns = $self->columns;
    my $result = bless {
    map {
        my $attr = $columns->{$_};
        ($attr->storage_key, $args->{$_} )} keys %$columns
    }, $self->class;
    $result;
}


=item attribute_to_column

=cut

sub attribute_to_column {
    my ($self, $attribute_name) = @_;
    my $columns = $self->columns;
    my $result = $attribute_name;
    foreach my $k (keys %$columns) {
        return $k
        if($columns->{$k}->accessor eq $attribute_name);
    }
    return $result;
}


=item map_attributes_to_column_values

Maps keys from passed in hash to coresponding columns.

=cut

sub map_attributes_to_column_values {
    my ($self, %args) = @_;
    my $attribute_to_column_map = $self->attribute_to_column_map;
    my %result;
    for my $k(keys %args) {
        my $column = $attribute_to_column_map->{$k} || $k;
        $result{$column} = $args{$k};
    }
    (%result);
}


=item attribute_to_column_map

=cut

sub attribute_to_column_map {
    my ($self, $attribute_name) = @_;
    my $columns = $self->columns;
    my $result = {};
    foreach my $k (keys %$columns) {
        $result->{$columns->{$k}->accessor} = $k;
    }
    return $result;
}


=item update_object

=cut

sub update_object {
    my ($self, $object, $column_values, $columns_to_update) = @_;
    my $columns = $self->columns;
    $columns_to_update ||= [keys %$column_values];
    for my $column_name (@$columns_to_update) {
        my $attribute = $columns->{$column_name} or next;
        $attribute->set_value($object, $column_values->{$column_name});
    }
}


=item join_columns_values

Returns join columns values for passed in relation

=cut

sub join_columns_values {
    my ($self, $entity, $relation_name, $object) = @_;
    my $relation = $entity->to_many_relationship($relation_name);
    my $pk_values = $self->column_values($object, $entity->primary_key);
    unless ($entity->has_primary_key_values($pk_values)) {
        my $values = $self->unique_values($object, $entity);
        $pk_values = $self->retrive_primary_key_values($values);
    }
    $entity->_join_columns_values($relation, $pk_values);
}


=item unique_values

Return unique columns values

=cut

sub unique_values {
    my ($self, $object, $entity) = @_;
    my @unique_columns = map { $_->name }  $entity->unique_columns;;
    $self->column_values($object, $entity->primary_key, @unique_columns);
}


=item primary_key_values

Return primary key values

=cut

sub primary_key_values {
    my ($self, $object, $entity) = @_;
    $self->column_values($object, $entity->primary_key);
}


=item trigger

=cut

sub trigger {
    my ($event_name, $code_ref) = @_;
    my $attr_class = 'Abstract::Meta::Attribute';
    my $package = caller();
    my $mapping_meta = mapping_meta($package) or confess "no entity defined for class $package";
    $mapping_meta->_trigger($event_name, $code_ref);
}


=item validate_trigger

Validates triggers types

=cut

{
    my @triggers = qw(before_insert after_insert before_update after_update before_delete after_delete on_fetch);
    sub validate_trigger {
        my ($self, $name, $value) = @_;
        confess "invalid trigger name: $name , must be one of " . join(",", @triggers)
            unless (grep {$name eq $_} @triggers);
        confess "secound parameter must be a callback"
            unless ref($value) eq 'CODE';
    }
}


=item run_event

=cut

sub run_event {
    my ($self, $name, @args) = @_;
    my $event = $self->_trigger($name);
    $event->($self, @args) if $event;
}


1;

__END__

=back

=head1 SEE ALSO

L<Abstract::Meta::Class>
L<Persistence::Entity::Manager>
L<SQL::Entity>

=head1 COPYRIGHT AND LICENSE

The SQL::Entity::ORM module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas, adrian@webapp.strefa.pl

=cut

1;
