# ------------------
# Data::LoggerSQL
# ------------------
package Data::LoggerSQL;

use 5.008008;
use strict;
no strict "refs";

use Time::Piece;

use warnings;

require Exporter;

use Data::Dumper;
use DBI;
use Carp;

use vars qw(@ISA);
@ISA = qw(Exporter DBI);

our $VERSION = 1.0;

# --------------------------
sub new
{
  my $class = shift @_;

  # Passing reference or hash
  my %arg_hsh;
  if ( ref($_[0]) eq "HASH" ) { %arg_hsh = %{ shift @_ } }
  else                        { %arg_hsh = @_ }

  # verify input
  unless ( ( $arg_hsh{'DSN'} && $arg_hsh{'USR'} ) ) { die("Must specify a DSN and USR") }

  my $Dbh = DBI->connect( $arg_hsh{'DSN'}, $arg_hsh{'USR'} ,$arg_hsh{'PSD'}, { PrintError => "0" , RaiseError => "0" } ) || die ("$DBI::errstr");

  $Dbh->{HandleError} = sub { print "Error! --------- $DBI::state\n"; confess(shift); }; # Handle errors

  my $self = {
        Dbh => $Dbh,
        tbl_info  => {},
        hdl_cache => {},
        args   => \%arg_hsh
    };
    bless $self, $class;
    return $self;
}

# - - - - - - - - - - - - - - - - - - -
# Log data
# - - - - - - - - - - - - - - - - - - -
sub log {

 my $self    = shift @_;
 my $tname   = shift @_;
 my $data_ref = shift @_;

 #my ( $col_lst, $val_lst, $nullable_lst ) = $self->get_cols_vals($tname,$data_ref);
 #print Dumper ( $col_lst, $val_lst, $nullable_lst );
 #exit;

 unless ( $data_ref->{time} ) { $data_ref->{time} = time }

 if ( not $self->{tbl_info}->{$tname} )
 {
    my $tbl_info = $self->get_table_info($tname);
    $self->{tbl_info}->{$tname} = $tbl_info;
 }

 my @field_lst = @{ $self->{tbl_info}->{$tname}->{NAME} };

 my $sql = sprintf "insert into $tname (%s) values (%s)", join(",", @field_lst), join(",", ("?")x@field_lst);

 my $sth = $self->{Dbh}->prepare_cached($sql);

 my @value_lst = @{$data_ref}{@field_lst};

 return $sth->execute(@value_lst)

}

# - - - - - - - - - - - - - - - - - - -
# Select Last row
# - - - - - - - - - - - - - - - - - - -
sub select_last {

 my $self      = shift @_;
 my $tname     = shift @_;
 my $fields = shift @_;
 my $rtn_type  = shift @_ || 'ARRAY';

 my $sql = "select $fields from $tname order by time DESC LIMIT 1";
 my $sth = $self->{Dbh}->prepare_cached($sql);

 $sth->execute();
 my $rtn_ref;
 if ( $rtn_type eq 'HASH' ) { $rtn_ref = $sth->fetchrow_hashref;  }
 else                       { $rtn_ref = $sth->fetchrow_arrayref; }
 $sth->finish();

 return $rtn_ref;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - -
#  $Db->select_col_past_now($tname, $fields, $past_sec );
# - - - - - - - - - - - - - - - - - - - - - - - - - -
sub select_col_past_now {

 my $self     = shift @_;
 my $tname    = shift @_;
 my $field    = shift @_;
 my $past     = shift @_;    # In seconds
 my $now = shift @_ || time;

 my @lst;

 my $past_now = $now - $past;

 my $sql = "select $field from $tname where time > ?";

 return $self->{Dbh}->selectcol_arrayref($sql,{},$past_now);


 #my @fields = split /,/,$field;

 #foreach my $field ( @fields )
 #{
 #   my $sql = "select ? from $tname where time > ?";
 #   my $rtn_ref->{$field} = $self->{Dbh}->selectcol_arrayref($sql);

    #my $sth = $self->{Dbh}->prepare_cached($sql);
    #$sth->execute($field, $past_now);
    #$rtn_ref = $sth->fetchall_array();
 #}
 #return $rtn_ref;
}

# - - - - - - - - - - - - - - - - - - - - - - - - - -
#  $hsh_ref = $Db->select_past_now($tname, $past_sec );
# - - - - - - - - - - - - - - - - - - - - - - - - - -
sub select_past_now {

 my $self     = shift @_;
 my $tname    = shift @_;
 my $past     = shift @_;    # In seconds
 my $now = shift @_ || time;

 my @lst;

 my $past_now = $now - $past;

 my $sql = "select * from $tname where time > ?";

 return $self->{Dbh}->selectall_arrayref($sql,{ Slice => {} },$past_now);
}

# - - - - - - - - - - - - - - - - - - - - - - - - - -
#  $Db->select_avg_past_now($tname, $fields, $past_sec );
# - - - - - - - - - - - - - - - - - - - - - - - - - -
sub select_avg_past_now {

 my $self     = shift @_;
 my $tname    = shift @_;
 my $field    = shift @_;
 my $past     = shift @_;    # In seconds
 my $now = shift @_ || time;

 my @lst;

 my $past_now = $now - $past;

 my $sql = "select AVG($field) from $tname where time > ?";

 return $self->{Dbh}->selectcol_arrayref($sql,{},$past_now);
}


# - - - - - - - - - - - - - - - - - - - - - - - - - -
#  $Db->select_span($tname,$fields,$t1,$t2,);
# - - - - - - - - - - - - - - - - - - - - - - - - - -
sub select_span {

 my $self   = shift @_;
 my $tname  = shift @_;
 my $fields = shift @_ || '*';
 my $t1 = shift @_;
 my $t2 = shift @_;

 # Time should be in format such as '2013-12-11 00:00:00'

 # select * from attitude where time > UNIX_TIMESTAMP('2013-12-11 00:00:00') AND time < UNIX_TIMESTAMP('2013-12-11 21:00:00');

 my $sql = "select $fields from $tname where time > UNIX_TIMESTAMP('$t1') AND time < UNIX_TIMESTAMP('$t2')";
 #main::debug($sql);
 my $ref =  $self->{Dbh}->selectall_arrayref($sql);

 return $ref;
}

# - - - - - - - - - - - - - - - - -
sub is_current
{
   my $self  = shift @_;
   my $table = shift @_;
   my $threshold = shift @_;

   my $ref = $self->select_last($table,'time');
   if ( abs( $ref->[0] - time ) <= $threshold ) { return 1 }

   return 0; # False
}

# - - - - - - - - - - - - - - - - -
sub get_channels
{
   my $self  = shift @_;
   my $table = shift @_;

   my $sql = "select * FROM $table order by sort";
   my $loh = $self->{Dbh}->selectall_arrayref($sql,{Columns=>{}});
 
   $self->{'channels'}->{$table} = $loh;
 
   return $loh;
}

# - - - - - - - - - - - - - - - - -
sub set_format
{
   my $self  = shift @_;
   my $table = shift @_;
   my $data_ref = shift @_;

   my $chan_table = "${table}_channels";

   my $loh = $self->{'channels'}->{$chan_table};
   unless ( $loh )
   {
      $loh = $self->get_channels($chan_table);
   }
  
   unless ( $loh ) { warning("No channel information found $chan_table"); return ""; }

   foreach my $chan_ref ( @$loh )
   {
      unless ( $chan_ref->{format}  ) { next; }
      unless ( $data_ref->{$chan_ref->{chan_id}}  ) { next; }
      
      $data_ref->{$chan_ref->{chan_id}} = sprintf("$chan_ref->{format}",$data_ref->{$chan_ref->{chan_id}});
   }

   return $data_ref;
}

# - - - - - - - - - - - - - - - - -
sub get_table_info
{
   my $self  = shift @_;
   my $table = shift @_;

   unless ($table) { die("No Table defined") }

   if ( $self->{table_info}->{$table} ) { return $self->{table_info}->{$table}; }

   my %hsh;
   # Get column name from table

   my $sth = $self->{Dbh}->prepare("SELECT * FROM $table LIMIT 1");
   $sth->execute();

   #if    ( $Mode =~ /mysql/i ) { $sth = $self->prepx("SELECT * FROM $table LIMIT 1") }
   #elsif ( $Mode =~ /odbc/i  ) { $sth = $self->prepx("SELECT TOP 1 * FROM $table")   }
   #else                        { debug("Mode $Mode not supported")             }

   $hsh{'NAME'} = $sth->{NAME};
   $hsh{'TYPE'} = $sth->{TYPE};
   $hsh{'PRECISION'} = $sth->{PRECISION};
   $hsh{'NULLABLE'}  = $sth->{NULLABLE};

   $hsh{'IS_NUM'}  = $sth->{'mysql_is_num'};   # Another mysql'sm

   return \%hsh;
}

# - - - - - - - - - - - - - - - - - -
# prepx() Prepares and execute a SQL statement
# - - - - - - - - - - - - - - - - - -
sub prepx
{
   my $self = shift(@_);
   my $sql  = shift(@_);

   my $sth = $self->{Dbh}->prepare($sql);
   my $tst = $sth->execute();

   return $sth;
}


__DATA__

