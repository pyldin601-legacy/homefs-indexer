#!/usr/bin/perl

use DBI;
use Digest::MD5 qw(md5_hex);
use Time::HiRes qw(time);
use Cwd;
use Encode;
use FindBin qw($Bin);
use Data::Dumper;


my $db_user = 'root';
my $db_pass = '';
my $db_host = 'localhost';
my $db_base = 'homefs';

my @target = ('/home/roman/Storage');

my $dsn = "dbi:mysql:$db_base:$db_host:3306";
my $dbh = DBI->connect($dsn, $db_user, $db_pass);

$dbh->do("SET NAMES 'utf8'");

foreach my $dir (@target) {
	scan_here($dir, undef);
}

$dbh->disconnect();


#################
#   FUNCTIONS   #
#################

sub scan_here
{
	my ($dir_name, $parent_id) = @_;
	my $this_id = current_dir_id($dir_name, $parent_id);
	my @dirs = (), @files = ();
	
	opendir(DIR, $dir_name);
	my @file_sort = readdir(DIR);
	closedir(DIR);
	
	@file_sort = sort(@file_sort);
	
	while(my $file = shift(@file_sort)){
		next if($file eq "." || $file eq "..");	# skip special files
		next if(substr($file, 0, 1) eq ".");	# skip hidden files
		my $tmp = $dir_name . "/" . $file;
		next if(-l $tmp);						# skip links
		if(-d $tmp && -x $tmp) {
			push(@dirs, $file);
		} elsif(-f $tmp && -r $tmp) {
			push(@files, $file);
		}
	}
	chdir($dir_name);
	foreach my $file (@files) {
		scan_file($file, $this_id);
	}
	foreach my $dir (@dirs) {
		scan_here($dir, $this_id);
	}
	chdir("..");
}

sub scan_file
{
	my ($name, $parent_id) = @_;
	my ($fsize) = (stat($name))[7];

	$q = $dbh->prepare("SELECT `id` FROM `files` WHERE `name` = ? AND `dir_id` = ?");
	$q->execute($name, $parent_id);
	if($q->rows == 1) {
		my ($id) = $q->fetchrow_array();
		$dbh->do("UPDATE `files` SET `size` = ? WHERE `id` = ?", undef, $fsize, $id);
	} else {
		$dbh->do("INSERT INTO `files` (`name`, `dir_id`, `size`) VALUES (?, ?, ?)", undef, $name, $parent_id, $fsize);
		$dbh->last_insert_id(undef, undef, "files", "id");
	}
	$q->finish();
}

sub current_dir_id
{
	my ($dir_name, $parent_id) = @_;
	
	my $id;
	$q = $dbh->prepare("SELECT `id` FROM `dirs` WHERE `name` = ? AND `parent_id` <=> ?");
	$q->execute($dir_name, $parent_id);
	
	if($q->rows()) {
		($id) = $q->fetchrow_array();
	} else {
		$dbh->do('INSERT INTO `dirs` (`name`, `parent_id`) VALUES (?, ?)', undef, $dir_name, $parent_id);
		$id = $dbh->last_insert_id(undef, undef, 'dirs', 'id');
	}
	
	$q->finish();
	
	return $id;
}
