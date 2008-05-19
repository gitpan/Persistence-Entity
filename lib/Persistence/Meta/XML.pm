package Persistence::Meta::XML;

use strict;
use warnings;
use vars qw($VERSION);

use Abstract::Meta::Class ':all';
use Carp 'confess';
use Persistence::Entity::Manager;
use Persistence::Entity ':all';
use Persistence::ORM;
use Persistence::Relationship;
use Persistence::Relationship::ToOne;
use Persistence::Relationship::OneToMany;
use Persistence::Relationship::ManyToMany;
use SQL::Entity::Condition;
use Simple::SAX::Serializer;
use Simple::SAX::Serializer::Handler ':all';

$VERSION = 0.01;

=head1 NAME

Persistence::Meta::XML - Persistence meta object xml injection

=cut

=head1 SYNOPSIS

   use Persistence::Meta::XML;
   my $meta = Persistence::Meta::XML->new(persistence_dir => 'meta/');
   my $entity_manager = $meta->inject('my_persistence.xml');
   #or
   # $meta->inject('my_persistence.xml');
   # my $entity_manager = Persistence::Entity::Manager->manager('manager_name');


=head1 DESCRIPTION

Loads xml files that containt meta persistence definition.

    persistence.xml
    <?xml version="1.0" encoding="UTF-8"?>
    <persistence name="test"  connection_name="test" >
        <entities>
            <entity_file  file="emp.xml"  />
            <entity_file  file="dept.xml" />
        </entities>
        <mapping_rules>
            <orm_file file="Employee.xml" />
            <orm_file file="Department.xml" />
        </mapping_rules>
    </persistence>

    emp.xml
    <?xml version="1.0" encoding="UTF-8"?>
    <entity name="emp" alias="e">
        <primary_key>empno</primary_key>
        <columns>
            <column name="empno" />
            <column name="ename" unique="1" />
            <column name="sal" />
            <column name="job" />
            <column name="deptno" />
        </columns>
        <subquery_columns>
            <subquery_column name="dname" entity="dept" />
        </subquery_columns>
    </entity>

    dept.xml
    <?xml version="1.0" encoding="UTF-8"?>
    <entity name="dept" alias="d">
        <primary_key>deptno</primary_key>
        <columns>
            <column name="deptno" />
            <column name="dname"  unique="1" />
            <column name="loc" />
        </columns>
        <to_many_relationships>
            <relationship target_entity="emp" order_by="deptno, empno">
                <join_column>deptno</join_column>
            </relationship>
        </to_many_relationships>
    </entity>

    Employee.xml
    <?xml version="1.0" encoding="UTF-8"?>
    <orm entity="emp"  class="Employee" >
        <column name="empno"  attribute="id" />
        <column name="ename"  attribute="name" />
        <column name="job"    attribute="job" />
        <column name="dname"  attribute="dept_name" />
        <to_one_relationship  name="dept" attribute="dept" fetch_method="EAGER" cascade="ALL"/>
    </orm>

    Department.xml
    <?xml version="1.0" encoding="UTF-8"?>
    <orm entity="dept"  class="Department" >
        <column name="deptno" attribute="id" />
        <column name="dname" attribute="name" />
        <column name="loc" attribute="location" />
        <one_to_many_relationship  name="emp" attribute="employees" fetch_method="EAGER" cascade="ALL"/>
    </orm>



    package Employee;
    use Abstract::Meta::Class ':all';

    has '$.id';
    has '$.name';
    has '$.job';
    has '$.dept_name';
    has '$.dept' => (associated_class => 'Department');

    package Department;
    use Abstract::Meta::Class ':all';

    has '$.id';
    has '$.name';
    has '$.location';
    has '@.employees' => (associated_class => 'Employee');

    my $meta = Persistence::Meta::XML->new(persistence_dir => $dir);
    my $entity_manager = $meta->inject('persistence.xml');

    my ($dept) = $entity_manager->find(dept => 'Department', name => 'dept3');

    my $enp = Employee->new(id => 88, name => 'emp88');
    $enp->set_dept(Department->new(id => 99, name => 'd99'));
    $entity_manager->insert($enp);

=head1 EXPORT

None

=head2 ATTRIBUTES

=over

=item entities

=cut

has '%.entities' => (item_accessor => 'entity');


=item _entities_subquery_columns

=cut

has '%._entities_subquery_columns';


=item _entities_to_many_relationship

=cut

has '%._entities_to_many_relationships';


=item _entities_to_one_relationship

=cut

has '%._entities_to_one_relationships';


=item cache_dir

Containts cache directory.

=cut

has '$.cache_dir';


=item use_cache

Flag that indicates if use cache.

=cut

has '$.persistence_dir';


=item persistence_dir

Contains directory of xml files that contain persistence object definition.

=cut


=back

=head2 METHODS

=over

=item initialise

=cut

sub inject {
    my ($self, $file) = @_;
    $self->_entities_subquery_columns({});
    $self->_entities_to_many_relationships({});
    $self->_entities_to_one_relationships({});
    my $xml = $self->persistence_xml_handler;
    my $prefix_dir = $self->persistence_dir;
    $xml->parse_file($prefix_dir . $file);
}


=item persistence_xml_handler

Retunds xml handlers that will transform the persistence xml into objects.
Persistence node is mapped to the Persistence::Entity::Manager;

<!ELEMENT persistence (entities+,mapping_rules*)>
<!ATTLIST persistence name #REQUIRED>
<!ATTLIST persistence connection_name #REQUIRED>
<!ELEMENT entities (entity_file+)>
<!ELEMENT entity_file (filter_condition_value+ .dml_filter_value) >
<!ATTLIST entity_file file id order_index>
<!ELEMENT mapping_rules (orm_file+)>
<!ATTLIST mapping_rules file>

<?xml version='1.0' encoding='UTF-8'?>
<persistence name="test"  connection_name="test" >
    <entities>
        <entity_file file="emp.xml"  />
        <entity_file file="dept.xml"  />
    </entities>
    <mapping_rules>
        <orm_file file="Employee" />
    </mapping_rules>
</persistence>

=cut

sub persistence_xml_handler {
    my ($self) = @_;
    my $xml = Simple::SAX::Serializer->new;
    $self->add_xml_persistence_handlers($xml);
    $xml;
}



=item add_xml_persistence_handlers

Adds persistence xml handlers/
Takes Simple::SAX::Serializer object as parameter.

=cut

sub add_xml_persistence_handlers {
    my ($self, $xml) = @_;

    my $temp_data = {};
    $xml->handler('persistence', root_object_handler('Persistence::Entity::Manager' , sub {
        my ($result) = @_;
        $self->load_persistence_object($result, $temp_data->{entities}, $temp_data->{orm});
        delete $temp_data->{$_} for qw(entities orm);
        $result;
    })),
    
    $xml->handler('entities', ignore_node_handler());
    $xml->handler('to_many_relationships', ignore_node_handler());
    $xml->handler('entity_file', custom_array_handler($temp_data, undef, undef, 'entities'));
    $xml->handler('filter_condition_values', hash_handler());
    $xml->handler('dml_filter_values', hash_handler());
    $xml->handler('mapping_rules', ignore_node_handler());
    $xml->handler('orm_file', custom_array_handler($temp_data, undef, undef, 'orm'));
    
}


=item load_persistence_object

Loads persistence object.
Takes entity manager object, array ref of the entity files, array ref of the ORM files.

=cut

sub load_persistence_object {
    my ($self, $entity_manager, $entity_files, $orm_files) = @_;
    my $entity_xml_hander = $self->entity_xml_handler;
    my $orm_xml_handler = $self->orm_xml_handler;
    my $prefix_dir = $self->persistence_dir;
    for my $entity_ref (@$entity_files) {
        my $file_name = $prefix_dir . $entity_ref->{file};
        my %overwriten_entity_attributes = (map { $_ ne 'file' ? ($_ => $entity_ref->{$_}) : ()} keys %$entity_ref);
        my $entity = $entity_xml_hander->parse_file($file_name, \%overwriten_entity_attributes);
        $self->entity($entity->id, $entity);
    }
    
    $self->_initialise_subquery_columns();
    $self->_initialise_to_one_relationships();
    $self->_initialise_to_many_relationships();
    
    my %entities = $self->entities;
    $entity_manager->add_entities(values %entities);
    for my $orm_ref (@$orm_files) {
        my $file_name = $prefix_dir . $orm_ref->{file};
        $orm_xml_handler->parse_file($file_name);
    }
    
}



=item orm_xml_handler

    <!ELEMENT orm (column+, to_one_relationship*, one_to_many_relationship*, many_to_many_relationship*)
    <!ATTRLIST orm class entity>
    <!ELEMENT column>
    <!ATTRLIST column name attribute>
    <!ELEMENT to_one_relationship>
    <!ATTRLIST to_one_relationship name attribute #REQUIRED>
    <!ATTRLIST to_one_relationship fetch_method (LAZY|EAGER) "LAZY">
    <!ATTRLIST to_one_relationship cascade (NONE|ALL|ON_INSERT|ON_UPDATE|ON_DELETE) "NONE">

    <orm entity="emp"  class="Employee" >
        <column name="empno" attribute="id" />
        <column name="ename" attribute="name" />
        <column name="job" attribute="job" />
        <to_one_relationship name="dept" attribute="depts" fetch_method="LAZY" cascade="ALL">
    </orm>

    many_to_many 'project' => (
        attribute        => has('%.projects' => (associated_class => 'Project'), index_by => 'name'),
        join_entity_name => 'emp_project',
        fetch_method     => LAZY,
        cascade          => ALL,
    );


=item orm_xml_handler

Retunds xml handlers that will transform the orm xml into Persistence::ORM object

=cut

sub orm_xml_handler {
    my ($self) = @_;
    my $xml = Simple::SAX::Serializer->new;
    $self->add_orm_xml_handlers($xml);
    $xml;
}


=item add_orm_xml_handlers

Adds orm xml handler to Simple::SAX::Serializer object.

=cut

sub add_orm_xml_handlers {
    my ($self, $xml) = @_;
    my $temp_data = {};
    $xml->handler('orm', sub {
        my ($this, $element, $parent) = @_;
        my $attributes = $element->attributes;
        my $children_result = $element->children_result || {};
        $self->create_orm_mapping($attributes, $children_result)
    });
    $xml->handler('column', hash_of_array_handler(undef, undef, 'columns'));
    $xml->handler('to_one_relationship', hash_of_array_handler(undef, undef, 'to_one_relationships'));
    $xml->handler('one_to_many_relationship', hash_of_array_handler(undef, undef, 'one_to_many_relationships'));
    $xml->handler('many_to_many_relationship', hash_of_array_handler(undef, undef, 'many_to_many_relationships'));
}


=item create_orm_mapping

Creates orm mappings.
Takes 

=cut

sub create_orm_mapping {
    my ($self, $args, $rules) = @_;
    my $columns = $rules->{columns};
    my $to_one_relationships = $rules->{to_one_relationships};
    my $one_to_many_relationships = $rules->{one_to_many_relationships};
    my $many_to_many_relationships = $rules->{many_to_many_relationships};
    $args->{entity_name} = $args->{entity}, delete $args->{entity};
    my $orm = Persistence::ORM->new(%$args);
    for my $column (@$columns) {
         $orm->add_column($column->{name}, $column->{attribute});
    }
    for my $relation (@$to_one_relationships) {
        $self->_add_to_one_relationship($relation, $orm);
    }
    for my $relation (@$one_to_many_relationships) {
        $self->_add_one_to_many_relationship($relation, $orm);
    }
    for my $relation (@$many_to_many_relationships) {
        $self->_add_many_to_many_relationship($relation, $orm);
    }
    
    $orm;
}



=item _add_one_to_many_relationship

=cut

sub _add_one_to_many_relationship {
    my ($self, $relationship, $orm) = @_;
    Persistence::Relationship::OneToMany->add_relationship($self->_add_relationship_parameters($relationship, $orm));
}



=item _add_to_many_to_many_relationship

=cut

sub _add_many_to_many_relationship {
    my ($self, $relationship, $orm) = @_;
    Persistence::Relationship::ManyToMany->add_relationship($self->_add_relationship_parameters($relationship, $orm));
}


=item _add_to_one_relationship

=cut

sub _add_to_one_relationship {
    my ($self, $relationship, $orm) = @_;
    Persistence::Relationship::ToOne->add_relationship($self->_add_relationship_parameters($relationship, $orm));
}


=item _add_relationship_parameters

=cut

sub _add_relationship_parameters {
    my ($self, $relationship, $orm) = @_;
    my $attribute = $orm->attribute($relationship->{attribute});
    
    my @result = ($orm->class, $relationship->{name}, attribute => $attribute);
    if (my $fetch_method = $relationship->{fetch_method}) {
        push @result, 'fetch_method' => Persistence::Relationship->$fetch_method();
    }
    if (my $cascade = $relationship->{cascade}) {
        push @result, 'cascade' => Persistence::Relationship->$cascade();
    }
    
    if (my $join_entity = $relationship->{join_entity}) {
        push @result, 'join_entity_name' => $join_entity;
    }
    @result;
}




=item entity_xml_handler

Retunds xml handlers that will transform the enity xml into Persistence::Entity

<!ELEMENT entity (primary_key*, indexes?, columns?, subquery_columns?,
filter_condition_value+ .dml_filter_value+, to_one_relationships? to_many_relationships?, value_generators*)>
<!ATTLIST entity id name alias unique_expression query_from schema order_index>
<!ELEMENT primary_key (#PCDATA)>
<!ELEMENT indexes (index+)>
<!ELEMENT index (index_columns+)>
<!ATTLIST index name hint>
<!ELEMENT index_columns (#PCDATA)>
<!ELEMENT columns (column+) >
<!ELEMENT subquery_columns (subquery_column+)>
<!ELEMENT subquery_column>
<!ATTLIST subquery_column entity name>
<!ELEMENT column>
<!ATTLIST column id name unique expression case_sensitive queryable insertable updatable>
<!ELEMENT filter_condition_values (#PCDATA)>
<!ATTLIST filter_condition_values name #REQUIRED>
<!ELEMENT dml_filter_values (#PCDATA)>
<!ATTLIST dml_filter_values name #REQUIRED>
<!ELEMENT to_one_relationships (relationship+)>
<!ELEMENT to_many_relationships (relationship+)>
<!ELEMENT relationship (join_columns*, condition?)>
<!ATTLIST relationship  name target_entity order_by>
<!ELEMENT join_columns (#PCDATA)>
<!ELEMENT condition (condition+) >
<!ATTLIST condition operand1  operator operand2 relation>
<!ELEMENT value_generators (#PCDATA)>

For instnace.
<?xml version="1.0" encoding="UTF-8"?>
<entity name="emp" alias="e">
    <primary_key>empno</primary_key>
    <indexes>
        <index name="emp_idx_empno" hint="INDEX_ASC(e emp_idx_empno)">
            <index_column>ename</index_column>
        </index>
        <index name="emp_idx_ename">
            <index_column>empno</index_column>
        </index>
    </indexes>
    <columns>
        <column name="empno" />
        <column name="ename" />
    </columns>
    <subquery_columns>
        <subquery_column name="dname" entity_id="dept" />
    </subquery_columns>
    <to_one_relationships>
        <relationship target_entity="dept">
            <join_column>deptno</join_column>
        </relationship>
    </to_one_relationships>
</entity>

=cut

#TODO add xml schema with namespace


sub entity_xml_handler {
    my ($self) = @_;
    my $xml = Simple::SAX::Serializer->new;
    $self->add_entity_xml_handlers($xml);
    $xml;
}


=item add_entity_xml_handlers

Adds entity xml handler to the Simple::SAX::Serializer object.

=cut

sub add_entity_xml_handlers {
    my ($self, $xml) = @_;
    my $temp_data = {};
    my $entities_subquery_columns = $self->_entities_subquery_columns;
    my $entities_to_many_relationships = $self->_entities_to_many_relationships;
    my $entities_to_one_relationships = $self->_entities_to_one_relationships;
    $xml->handler('entity', root_object_handler('Persistence::Entity' , sub {
        my ($result) = @_;
        my $id = $result->id;
        $entities_subquery_columns->{$id} = \@{$temp_data->{subquery_columns}};
        $entities_to_many_relationships->{$id} = \@{$temp_data->{to_many_relationships}};
        $entities_to_one_relationships->{$id} = \@{$temp_data->{to_one_relationships}};
        delete $temp_data->{$_} for qw(subquery_columns to_many_relationships to_one_relationships);
        $result;
    })),
    $xml->handler('columns', hash_item_of_child_value_handler());
    $xml->handler('columns/column', array_of_objects_handler(\&sql_column));
    $xml->handler('indexes', hash_item_of_child_value_handler());
    $xml->handler('index', array_of_objects_handler(\&sql_index));
    $xml->handler('index_column', array_handler('columns'));
    $xml->handler('primary_key', array_handler());
    $xml->handler('value_generators', hash_handler());
    $xml->handler('filter_condition_values', hash_handler());
    $xml->handler('dml_filter_values', hash_handler());
    $xml->handler('subquery_columns', ignore_node_handler());
    $xml->handler('subquery_column', custom_array_handler($temp_data, undef, undef, 'subquery_columns'));
    $xml->handler('to_one_relationships', ignore_node_handler());
    $xml->handler('to_many_relationships', ignore_node_handler());
    $xml->handler('to_many_relationships/relationship', custom_array_handler($temp_data, undef, undef, 'to_many_relationships'));
    $xml->handler('to_one_relationships/relationship', custom_array_handler($temp_data, undef, undef, 'to_one_relationships'));
    $xml->handler('join_column', array_handler('join_columns'));
    $xml->handler('condition', object_handler('SQL::Entity::Condition'));
    $xml->handler('condition/condition', hash_of_object_array_handler('SQL::Entity::Condition', undef, undef, 'conditions'));
}



=item _initialise_subquery_columns

Initialise subquery columns

=cut

sub _initialise_subquery_columns {
    my ($self) = @_;
    my $entities = $self->entities;
    my $entities_subquery_columns = $self->_entities_subquery_columns;
    for my $entity_id (keys %$entities_subquery_columns) {
        my $entity = $entities->{$entity_id};
        my @subquery_columns;
        my $subquery_columns = $entities_subquery_columns->{$entity_id};
        for my $column_definition (@$subquery_columns) {
            push @subquery_columns,
                $self->entity_column($column_definition->{entity}, $column_definition->{name});
        }
        $entity->add_subquery_columns(@subquery_columns)
            if @subquery_columns;
    }
}


=item _initialise_to_one_relationship

Initialise to one relationships

=cut

sub _initialise_to_one_relationships {
    my ($self) = @_;
    $self->_initialise_relationships('to_one_relationships');
}


=item _initialise_to_many_relationship

Initialise to manye relationships

=cut

sub _initialise_to_many_relationships {
    my ($self) = @_;
    $self->_initialise_relationships('to_many_relationships');
}


=item _initialise_relationships

Initialises relationshsips
Takes relationship type as parameters.
Allowed value: 'to_one_relationships', 'to_many_relationships'

=cut

sub _initialise_relationships {
    my ($self, $relationship_type) = @_;
    my $entities = $self->entities;
    my $relationship_accessor = "_entities_${relationship_type}";
    my $entities_relationships = $self->$relationship_accessor;
    my $mutator = "add_${relationship_type}";

    for my $entity_id (keys %$entities_relationships) {
        my $entity = $entities->{$entity_id};
        my @relationships;
        my $relationships = $entities_relationships->{$entity_id};
        
        for my $relationship (@$relationships) {
            push @relationships, $self->_relationship($relationship);
        }
        
        if (@relationships) {
            $entity->$mutator(@relationships)
        }
            
    }
}


=item _relationship

Returns the relationship object.
Takes hash_ref, that will be transformed to the new object parameters.

=cut

sub _relationship {
    my ($self, $relationship) = @_;
    my $entity = $self->entity($relationship->{target_entity})
        or confess "unknow entity " . $relationship->{target_entity};
    $relationship->{target_entity} = $entity;
    my $condition = $relationship->{condition};
    $self->_parse_condition($condition) if $condition;
    sql_relationship(%$relationship);
}


=item _parse_condition

Parses condition object to replacase ant occurence of  <entity>.<column> to column object.

=cut

sub _parse_condition {
    my ($self, $condition) = @_;
    {
        my $operand1 = $condition->operand1;
        my ($entity, $column) = $self->has_column($operand1);
        $condition->set_operand1($self->entity_column($entity, $column)) if($column)
    }
    
    {
        my $operand2 = $condition->operand2;
        my ($entity, $column) = $self->has_column($operand2);
        $condition->set_operand2($self->entity_column($entity, $column)) if($column)
    }
    
    my $conditions = $condition->conditions;
    for my $k (@$conditions) {
        $self->_parse_condition($k);
    }
    
}


=item has_column

=cut

sub has_column {
    my ($self, $text) = @_;
    ($text =~ m /^sql_column:(\w+)\.(\w+)/);
}

=item entity_column

Returns entity column

=cut

sub entity_column {
    my ($self, $entity_id, $column_id) = @_;
    my $entities = $self->entities;
    my $entity = $entities->{$entity_id}
        or confess "unknown entity: ${entity_id}";
    my $column = $entity->column($column_id)
        or confess "unknown column ${column_id} on entity ${entity_id}";
}


1;

__END__

=back

=head1 SEE ALSO

L<Simple::SAX::Handler>

=head1 COPYRIGHT AND LICENSE

The Persistence::Meta::Xml module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 AUTHOR

Adrian Witas,adrian@webapp.strefa.pl

=cut

1;
