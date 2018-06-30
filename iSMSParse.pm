###########################################################################

package iSMSParse;

###########################################################################

use strict;
use Path::Class;
use Digest::SHA1  qw(sha1_hex);
use POSIX qw(strftime);

sub digest
{
	my $path = shift;
	my $domain = shift;
	$path =~ s!^(/var/mobile/|~/)!!;
	my $key = "$domain-$path";
	return sha1_hex($key)
}

sub encodedSubPath
{
	my $path = shift;
	my $domain = shift;
	my $digest = digest($path, $domain);
	return dir(substr($digest, 0, 2), $digest);
}

sub dateString
{
	my $val = shift;
	my $fmt = shift || '%Y%m%d-%H:%M:%S';
	return "" if !defined($val) || $val eq "0";
	if ($val =~ /^(\d{7,})0{7,7}$/) {
		$val = $1;
		my $extra = $2; # Why the 7 extra zeroes?
	}
	my ($sec,$min,$hour,$day,$month,$yr19,@rest) = localtime($val);
	return strftime($fmt, $sec,$min,$hour,$day,$month,$yr19);
}

sub isDateKey
{
	my $key = shift;
	return 1 if $key =~ /^date/;
	return 1 if $key =~ /^time/;
	return 1 if $key eq "last_read_message_timestamp";
	return 0;
}

sub fixDate
{
	my $d = shift;
	return undef if $d == 0;
	if ($d > 1e11) {
		$d /= 1e9;
	}
	return $d + 978307200;
}

sub fixValue
{
	my $key = shift;
	my $val = shift;
	if (isDateKey($key)) {
		return dateString(fixDate($val));
	} else {
		return $val;
	}
}

###########################################################################

package iSMSParse::Backup;

###########################################################################

use strict;
use Path::Class;

sub new
{
	my $class = shift;
	my $path = shift;
	die "Invalid path: $path" if !-d $path;
	my $self = {};
	bless($self, $class);
	$self->{path} = $path;
	return $self;
}

sub date
{
	my $self = shift;
	if (!defined($self->{date})) {
		$self->{date} = (stat($self->{path}))[9];
	}
	return $self->{date};
}

sub path
{
	my $self = shift;
	return $self->{path};
}

sub contacts
{
	my $self = shift;
	my $dbPath = iSMSParse::Contacts::makePath($self->{path});
	return iSMSParse::Contacts->new($dbPath);
}

sub sms
{
	my $self = shift;
	my $rootPath = $self->{path};
	my $dbPath = iSMSParse::SMS::makePath($rootPath);
	return iSMSParse::SMS->new($dbPath, $rootPath);
}

sub list
{
	my @ans;
	my $dir = $ENV{'APPDATA'} . '\\Apple Computer\\MobileSync\\Backup';
	opendir(DIRH, $dir);
	my @files = readdir(DIRH);
	closedir(DIRH);
	foreach my $f (@files) {
		next if $f eq '.' || $f eq '..';
		my $path = dir($dir, $f);
		push @ans, iSMSParse::Backup->new($path);
	}
	return (sort { $b->date <=> $a->date} @ans)
}

###########################################################################

package iSMSParse::Database;

###########################################################################

use strict;

use DBI;
use Path::Class;

my $sqlite_path = "F:\\Installers\\sqlite-tools-win32-x86-3160200";
my $sqlite_app = $sqlite_path . "\\sqlite3";

#   -bail                stop after hitting an error

#   -ascii               set output mode to 'ascii'
#   -column              set output mode to 'column'
#   -csv                 set output mode to 'csv'
#   -html                set output mode to HTML
#   -line                set output mode to 'line'
#   -list                set output mode to 'list'

#   -newline SEP         set output row separator. Default: '\n'
#   -separator SEP       set output column separator. Default: '|'
#   -nullvalue TEXT      set text string for NULL values. Default ''

#   -batch               force batch I/O
#   -interactive         force interactive I/O

#   -cmd COMMAND         run "COMMAND" before reading stdin
#   -echo                print commands before execution
#   -init FILENAME       read/process named file
#   -[no]header          turn headers on or off
#   -help                show this message

#   -mmap N              default mmap size set to N
#   -lookaside SIZE N    use N entries of SZ bytes for lookaside memory
#   -pagecache SIZE N    use N slots of SZ bytes each for page cache memory
#   -scratch SIZE N      use N slots of SZ bytes each for scratch memory

#   -stats               print memory stats before each finalize
#   -version             show SQLite version
#   -vfs NAME            use NAME as the default VFS

sub new {
	my $class = shift;
	my $dbfile = shift;
	
	die "Database doesn't exist: $dbfile" if !-f $dbfile;
	my $self = {
		dbfile => $dbfile
	};
	# print '"', $self->{dbfile}, '"', "\n";
	bless $self, $class;
	
	return $self;
}

sub path {
	my $self = shift;
	return $self->{dbfile};
}

sub databaseCommand {
	my $self = shift;
	return "sqlite3 \"$self->{dbfile}\"";
}

sub run_db_query {
	my $self = shift;
	my $query = shift;
	my $headers = shift;
	my @ans;

	my $f = $self->{dbfile};
	$f =~ s/\\/\//g;
	my $dbh = DBI->connect_cached("DBI:SQLite:dbname=$f", '', '', { RaiseError => 1 }) or die $DBI::errstr;
	$dbh->{RowCacheSize} = 0;

    # TODO - comment out debugging
    # $query = $query . " ORDER BY ROWID DESC";
	# print $query, "\n";
	# my $count = 0;

	my $sth = $dbh->prepare($query);
	$sth->execute() || die "Query '$query' failed: " . $DBI::errstr;
	my @headers = @{$sth->{NAME}};
	while (my @columns = $sth->fetchrow_array()) {
		my %data;
		die "column count($#columns) didn't match header count($#headers)" if $#columns != $#headers;
		for (my $i = 0; $i <= $#columns; ++$i) {
			# TODO - remove debugging
			# print STDOUT defined($columns[$i]) ? $columns[$i] : "undefined", "\n";
			$data{$headers[$i]} = $columns[$i];
			# if ($columns[$i] =~ /SOMETHING/) {
			# 	map { print "$headers[$i]:" . ord($_) . "\n" } split ('', $columns[$i]);
			# }
		}
		push @ans, \%data;
		# TODO - remove debugging
		# last if ++$count >= 2;
	}
	$dbh->disconnect();
	# TODO - remove debugging
	# die if $count >= 2;
	return @ans;
}

sub table {
	my $self = shift;
	my $table = shift;
	my $condition = shift | "";
	my $limit = shift | "";
	# -header
	my $query = "SELECT * FROM $table $condition $limit";
	return $self->run_db_query($query, 1);
}

# SELECT att.* FROM join JOIN att on join.attachmentid = attachmenttable.tattachid where join.messageid = messageid

sub table_join {
	my $self = shift;
	my $table1 = shift;
	my $field1 = shift;
	my $table2 = shift;
	my $field2 = shift;
	my $condition = shift | "";
	my $limit = shift | "";
	# -header
	my $query = "SELECT $table1.* FROM $table2 JOIN $table1 ON $table1.$field1 = $table2.$field2 $condition $limit";
	return $self->run_db_query($query, 1);
}
	# return map { iSMSParse::Contacts::Value->new($self->{db}, $_) } $self->table_join("handle", "ROWID", "chat_handle_join", "handle_id, "WHERE chat_handle_join.chat_id=$rowid");

sub DESTROY {
	my $self = shift;
	# clean up
}

###########################################################################

package iSMSParse::SMS;

###########################################################################

our @ISA = ('iSMSParse::Database');

use strict;

use Path::Class;

sub new
{
	my $class = shift;
	my $dbPath = shift;
	my $rootPath = shift;

	my $self = $class->SUPER::new($dbPath);
	$self->{HANDLE_CACHE} = {};
	$self->{_rootPath} = $rootPath if $rootPath;
	return $self;
}

sub chats
{
	my $self = shift;
	return map { iSMSParse::SMS::Chat->new($self->{_rootPath}, $self, $_) } $self->table("chat", undef); # , 'LIMIT 30');
}

sub messages
{
	my $self = shift;
	return map { iSMSParse::SMS::Message->new(undef, $self->{_rootPath}, $self, $_) } $self->table("message", undef); # , 'LIMIT 30');
}

sub handles
{
	my $self = shift;
	return map { iSMSParse::Record->new(undef, $_) } $self->table("handle", undef);
}

sub handle
{
	my $self = shift;
	my $handle_id = shift;

	if (!$self->{HANDLE_CACHE}->{$handle_id}) {
		my @tmp = map { iSMSParse::Record->new($self->{db}, $_) } $self->table("handle", "WHERE ROWID=$handle_id");
		$self->{HANDLE_CACHE}->{$handle_id} = $tmp[0];
	}
	return $self->{HANDLE_CACHE}->{$handle_id};
}

sub makePath {
	my $rootPath = shift;
	return file($rootPath, iSMSParse::encodedSubPath("Library/SMS/sms.db", "HomeDomain"));
}

###########################################################################

package iSMSParse::Contacts;

###########################################################################

our @ISA = ('iSMSParse::Database');

use strict;

use Path::Class;

sub new
{
	my $class = shift;
	my $dbPath = shift;
	
	my $self = $class->SUPER::new($dbPath);
	return $self;
}

sub people
{
	my $self = shift;

	return map { iSMSParse::Contacts::Person->new($self, $_) } $self->table("ABPerson", undef); # , 'LIMIT 30');
}

sub valueLabelName
{
	my $self = shift;
	my $label = shift;
	my $labelnum = int($label);
	return "phone" if ($label eq "");
	if (!$self->{valueLabels}) {
		my @valueLabels = map { iSMSParse::Record->new($self, $_) } $self->table("ABMultiValueLabel");
		$self->{valueLabels} = \@valueLabels;
	}
	if ($labelnum >= 0 + 1 && $labelnum <= $#{$self->{valueLabels}} + 1) {
		my $name = $self->{valueLabels}->[$labelnum - 1]->value;
		$name =~ s/^_\$!<(.*)>!\$_/\1/;
		$name = lc $name;
		return $name;
	} else {
		return "label $label";
	}
}

sub makePath {
	my $rootPath = shift;
	return file($rootPath, iSMSParse::encodedSubPath("Library/AddressBook/AddressBook.sqlitedb", "HomeDomain"));
}

###########################################################################

package iSMSParse::Record;

###########################################################################

use strict;

sub new()
{
	my $class = shift;
	my $db = shift;
	my $dataref = shift;
	
	my $self = {
		db => $db,
		data => $dataref
	};
	bless $self, $class;
	
	return $self;
}

sub table()
{
	my $self = shift;
	return $self->{db}->table(@_);
}

sub table_join()
{
	my $self = shift;
	return $self->{db}->table_join(@_);
}

sub isDate {
	my $self = shift;
	return 0;
}

sub hasKey {
	my $self = shift;
	my $called = shift;
	return exists $self->{data}->{$called};
}

sub keys {
	my $self = shift;
	return keys(%{$self->{data}});
}

our $AUTOLOAD;

sub AUTOLOAD {
	my $self = shift;
#	my $called = $AUTOLOAD =~ s/.*:://r;
	my $called = $AUTOLOAD;
	$called =~ s/.*:://;
	
	die "No such attribute: $called" unless exists $self->{data}->{$called};
		
	return $self->{data}->{$called};
}

###########################################################################

package iSMSParse::Contacts::Person;

###########################################################################

our @ISA = ('iSMSParse::Record');

use strict;

sub new
{
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	return $self;
}

sub values {
	my $self = shift;

	my $rowid = $self->ROWID;
	return map { iSMSParse::Contacts::Value->new($self->{db}, $_) } $self->table("ABMultiValue", "WHERE record_id=$rowid");
}

###########################################################################

package iSMSParse::Contacts::Value;

###########################################################################

our @ISA = ('iSMSParse::Record');

use strict;

sub new
{
	my $class = shift;
	my $self = $class->SUPER::new(@_);
	return $self;
}

sub type
{
	my $self = shift;
	return $self->{db}->valueLabelName($self->label);
}

###########################################################################

package iSMSParse::SMS::Chat;

###########################################################################

our @ISA = ('iSMSParse::Record');

use strict;

sub new
{
	my $class = shift;
	my $rootPath = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_rootPath} = $rootPath;
	return $self;
}

sub handles {
	my $self = shift;
	my $rowid = $self->ROWID;
	return map { iSMSParse::Record->new($self->{db}, $_) } $self->table_join("handle", "ROWID", "chat_handle_join", "handle_id", "WHERE chat_handle_join.chat_id=$rowid");
}

sub messages {
	my $self = shift;
	my $rowid = $self->ROWID;
	return map { iSMSParse::SMS::Message->new($self, $self->{_rootPath}, $self->{db}, $_) } $self->table_join("message", "ROWID", "chat_message_join", "message_id", "WHERE chat_message_join.chat_id=$rowid ORDER BY message.date ASC");
}

###########################################################################

package iSMSParse::SMS::Message;

###########################################################################

our @ISA = ('iSMSParse::Record');

use strict;

sub new
{
	my $class = shift;
	my $chat = shift;
	my $rootPath = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_chat} = $chat;
	$self->{_rootPath} = $rootPath;
	return $self;
}

sub chats
{
	my $self = shift;
	if (!defined($self->{_chat})) {
		my $msgId = $self->ROWID;
		return map { iSMSParse::SMS::Chat->new($self->{_rootPath}, $self->{db}, $_) } $self->table_join("chat", "ROWID", "chat_message_join", "chat_id", "WHERE chat_message_join.message_id=$msgId");
		# return map { iSMSParse::SMS::Chat->new($self->{_rootPath}, $self->{db}, $_) } $self->table_join("message", "ROWID", "chat_message_join", "message_id", "WHERE chat_message_join.chat_id=$rowid ORDER BY message.date ASC");
		# return map { iSMSParse::SMS::Chat->new($self->{_rootPath}, $self, $_) } $self->table("chat", undef); # , 'LIMIT 30');
	} else {
		return ($self->{_chat});
	}
}

sub attachments {
	my $self = shift;
	my $rowid = $self->ROWID;
	return map { iSMSParse::SMS::Attachment->new($self->{_rootPath}, $self->{db}, $_) } $self->table_join("attachment", "ROWID", "message_attachment_join", "attachment_id", "WHERE message_attachment_join.message_id=$rowid");
}

sub isDate {
	my $self = shift;
	my $prop = shift;
	return 1 if $prop eq "date" || $prop eq "date_read" || $prop eq "date_delivered";
	return 1 if $prop eq "time_expressive_send_played";
	return 0;
}

sub handle {
	my $self = shift;
	return $self->{db}->handle($self->handle_id);
}

sub otherHandle {
	my $self = shift;
	return $self->{db}->handle($self->other_handle);
}


###########################################################################

package iSMSParse::SMS::Attachment;

###########################################################################

our @ISA = ('iSMSParse::Record');

use strict;

use Path::Class;

sub new
{
	my $class = shift;
	my $rootPath = shift;
	my $self = $class->SUPER::new(@_);
	$self->{_rootPath} = $rootPath;
	return $self;
}

sub path
{
	my $self = shift;
	my $filename = $self->filename;
	return file($self->{_rootPath}, iSMSParse::encodedSubPath($filename, "MediaDomain"));
}

###########################################################################

1;
