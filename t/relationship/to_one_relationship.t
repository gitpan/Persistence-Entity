use strict;
use warnings;

use Test::More tests => 18;
use Test::DBUnit connection_name => 'test';

my $class;
my $fetch_counter;
BEGIN {
    $class = 'Persistence::Relationship::ToOne';
    use_ok($class, ':all');
    use_ok('Persistence::Entity::Manager');
    use_ok('Persistence::ORM', ':all');
    use_ok('Persistence::Entity', ':all');
}


my $entity_manager = Persistence::Entity::Manager->new(name => 'my_manager', connection_name => 'test');

{
    my $emp_entity = Persistence::Entity->new(
        name    => 'emp',
        alias   => 'ep',
        primary_key => ['empno'],
        columns => [
            sql_column(name => 'empno'),
            sql_column(name => 'ename', unique => 1),
            sql_column(name => 'job'),
            sql_column(name => 'deptno'),
        ],
    );

    my $dept_entity = Persistence::Entity->new(
        name    => 'dept',
        alias   => 'dt',
        primary_key => ['deptno'],
        columns => [
            sql_column(name => 'deptno'),
            sql_column(name => 'dname'),
            sql_column(name => 'loc')
        ],
    );
    
    $dept_entity->add_to_many_relationships(sql_relationship(target_entity => $emp_entity, join_columns => ['deptno'], order_by => 'deptno, empno'));
    $entity_manager->add_entities($dept_entity, $emp_entity);
    
    
}


{
    package Department;
    
    use Abstract::Meta::Class ':all';
    use Persistence::Entity ':all';
    use Persistence::ORM ':all';

    entity 'dept';
    column deptno => has('$.id');
    column dname  => has('$.name');
    column loc  => has('$.location');
   
}


{
    package Employee;
    
    use Abstract::Meta::Class ':all';
    use Persistence::Entity ':all';
    use Persistence::ORM ':all';
    
    entity 'emp';
    column empno=> has('$.id');
    column ename => has('$.name');
    column job => has '$.job';
    my $relation = to_one 'dept' => (
        attribute        =>  has ('$.dept', associated_class => 'Department'),
        cascade          => ALL,
        fetch_method     => EAGER,
    );

    ::is_deeply([$class->insertable_to_one_relations('Employee')], [$relation], 'should have insertable relations');
    ::is_deeply([$class->updatable_to_one_relations('Employee')],  [$relation],  'should have updatable relations');
    ::is_deeply([$class->deleteable_to_one_relations('Employee')], [$relation], 'should have deleteable relations');

}

SKIP: {
    
    ::skip('missing env varaibles DB_TEST_CONNECTION, DB_TEST_USERNAME DB_TEST_PASSWORD', 11)
      unless $ENV{DB_TEST_CONNECTION};

    my $connection = DBIx::Connection->new(
      name     => 'test',
      dsn      => $ENV{DB_TEST_CONNECTION},
      username => $ENV{DB_TEST_USERNAME},
      password => $ENV{DB_TEST_PASSWORD},
    ); 


    reset_schema_ok("t/sql/". $connection->dbms_name . "/create_schema.sql");
    
    xml_dataset_ok('init');

    {   #fetch
        my ($emp) = $entity_manager->find(emp => 'Employee', name => 'emp2');
        my $dept = Department->new(id => 5, name => 'dept5', location   => 'loc5');
        is_deeply($emp->{'$.dept'}, $dept, 'should fetch eagerly association data')
    }
    
    #cascade on insert
    {
        
        my $emp = Employee->new(id => 21, name => 'emp21', job => undef);
        my $dept = Department->new(id => '50', name => 'dept50', location => 'loc50');
        $emp->set_dept($dept);
        $entity_manager->insert($emp);
    }
    
    expected_xml_dataset_ok('insert');


    #cascade on update 
    {
        my ($emp) = $entity_manager->find(emp => 'Employee', name => 'emp2');
        $emp->dept->set_location('Warsaw');
        $emp->set_job('Manager');
        $entity_manager->update($emp);
    }
    expected_xml_dataset_ok('update');

    xml_dataset_ok('update');
    {
        my ($emp) = $entity_manager->find(emp => 'Employee', name => 'emp2');
        my $dept = Department->new(id => '5', name => 'dept5', location => 'Warsaw');
        $emp->set_dept($dept);
        $entity_manager->update($emp);
    }
    expected_xml_dataset_ok('update');

    xml_dataset_ok('update');
    {
        my ($emp) = $entity_manager->find(emp => 'Employee', name => 'emp2');
        $emp->set_dept(undef);
        $entity_manager->update($emp);
    }
    expected_xml_dataset_ok('update2');
    
    xml_dataset_ok('init');
    
    #cascade on delete
    {
        my ($emp) = $entity_manager->find(emp => 'Employee', name => 'emp2');
        $entity_manager->delete($emp);
    }
    expected_xml_dataset_ok('delete');
}