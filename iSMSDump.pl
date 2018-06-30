use iSMSParse;
use strict;

my $field = shift @ARGV || undef;
my $fieldValue = @ARGV > 0 ? shift @ARGV : undef;
my $limit = shift @ARGV || 10;

my @backups = iSMSParse::Backup::list() or die "No backups found";

# -B --list-backups
# print join("\n", map { $_->date . " --- " . $_->path } @backups), "\n";

# -b --backup BACKUP
# $backup = findMatchingBackup(BACKUP)
my $backup = $backups[0];

# -h --orphans
# findOrphans($backup); die("abort");

# --sql
print "contacts\n", $backup->contacts->databaseCommand, "\n";
print "sms\n", $backup->sms->databaseCommand, "\n";

#foreach my $chat ($backup->sms->chats) {
#    foreach my $message ($chat->messages) {
#    	print join(',', $message->keys), "\n";
#    	die;
#    }
#}

if ($field && defined($fieldValue)) {
	my $outputCount = 0;
	foreach my $message (reverse $backup->sms->messages) {
		if ($message->$field eq $fieldValue) {
			print "Message: ", $message->ROWID, "\n";
			printMessage($message);
			# foreach my $key ($message->keys) {
			# 	my $val = $message->$key;
			# 	print "    $key = $val\n";
			# }
			$outputCount++;
		}
		last if $limit > 0 && $outputCount >= $limit;
	}
} else {
	printValueChart("Table: messages", $backup->sms, $field, $backup->sms->messages);
}

sub getValue {
	my $valRef = shift;
	if (defined($valRef->{min})) {
		return $valRef->{count} . "[" . iSMSParse::dateString($valRef->{min}) . "-" . iSMSParse::dateString($valRef->{max}) . "]";
	} else {
		return $valRef->{count};
	}
}

sub printValueChart {
	my $title = shift;
	my $db = shift;
	my $field = shift;

	print "*" x 40, "\n";
	print "$title", "\n" if $title;
	print "*" x 40, "\n";

	my %keyValues;
	my $recordCount = 0;
	my $total = 0;

	my $k = "share_direction";
	my $v = "";

	foreach my $record (@_) {
		$recordCount++;
		my $timestamp = undef;
		my $dateKey = 'date';
		if ($record->hasKey($dateKey)) {
			$timestamp = iSMSParse::fixDate($record->$dateKey);
		}
		foreach my $key ($record->keys) {
			my $val = $record->$key;
			if (!defined($keyValues{$key}->{$val})) {
				$keyValues{$key}->{$val} = { min => $timestamp, max => $timestamp, count => 1 };
			} else {
	
				# if ($key eq $k && $val eq $v) {
				# 	print "TIMESTAMP: ", $timestamp, "\n";
				# }
				$keyValues{$key}->{$val}->{count}++;
				if (defined($timestamp)) {
					if ($timestamp < $keyValues{$key}->{$val}->{min}) {
						$keyValues{$key}->{$val}->{min} = $timestamp;
					}
					if ($timestamp > $keyValues{$key}->{$val}->{max}) {
						$keyValues{$key}->{$val}->{max} = $timestamp;
					}
				}
			}
			# if ($key eq $k && $val eq $v) {
			# 	print "$k=$v -> ", "(", defined($timestamp), ")", " ", getValue($keyValues{$key}->{$val}), "\n";
			# }
			$total++;
		}
	#	print join(',', $record->keys), "\n";
	#	die if $total > 10000;
	#	die;
	#	last if $recordCount > 100;
	}
	# die "HERE";
	foreach my $key (sort (keys %keyValues)) {
		next if defined($field) && $key ne $field;
		my @vals = keys %{$keyValues{$key}};
		print "$key - ", scalar(@vals);
		# print "\t$vals[0]\n";
		# print "\t$vals[1]\n";
		# print "\t$vals[2]\n";
		if (scalar(@vals) == 1) {
			print "\t", join("\t", map { $_ . 'x' . $keyValues{$key}->{$_}->{count}} @vals);
		} elsif (scalar(@vals) < 5) {
			print "\t", join("\t", map { $_ . 'x' . getValue($keyValues{$key}->{$_})} @vals);
		} else {
			for (my $i = 0; $i < 5; ++$i) {
				print "\t", "example:", $vals[$i], "x", getValue($keyValues{$key}->{$vals[$i]});
			}
		}
		print "\n";
	}
	print "$recordCount - $total\n";
}

sub findOrphans {
	my $backup = shift;
	my %chatMessages;
	my $dupCount = 0;
	my $orphanCount = 0;
	my $safetyLimit = 20;
	foreach my $chat ($backup->sms->chats) {
	    foreach my $message ($chat->messages) {
	    	my $rowId = $message->ROWID;
	    	if (defined($chatMessages{$rowId})) {
	    		warn "MSG ", join(", ", $message->chats) if $dupCount == 0;
		    	$dupCount++;
	    		if ($dupCount <= $safetyLimit) {
			    	warn "Duplicate ROWID $rowId";
			    }
			}
	    	$chatMessages{$rowId}++;
	    }
	}
	foreach my $message ($backup->sms->messages) {
	   	my $rowId = $message->ROWID;
		if (!defined($chatMessages{$rowId})) {
    		warn "MSG ", join(", ", $message->chats) if $orphanCount == 0;
			$orphanCount++;
    		if ($orphanCount <= $safetyLimit) {
				warn "Orphaned ROWID $rowId";
				# printOrphan($message);
			}
		}
	}
	print "$dupCount messages connected to multiple chats\n";
	print "$orphanCount messages connected to no chats\n";
}

sub printOrphan {
	my $message = shift;
	foreach my $key ($message->keys) {
		my $val = $message->$key;
		print "    $key = $val\n";
	}
}

sub printMessage {
	my $message = shift;
	warn "MSG ", join(", ", $message->chats);
	my @chats = $message->chats;
	my $chat = $chats[0];
	print "CHAT KEYS\n";
	foreach my $key (sort ($chat->keys)) {
		my $val = iSMSParse::fixValue($key, $chat->$key);
		print "    $key = $val\n";
	}
	print "MESSAGE KEYS\n";
	foreach my $key (sort ($message->keys)) {
		my $val = iSMSParse::fixValue($key, $message->$key);
		print "    $key = $val\n";
	}
}

sub usage {
	my $msg = shift;
	print STDERR "$msg\n" if $msg;
	print STDERR "Usage: perl iSMSDump.pl [field fieldValue [limit]]\n";
	die;
}
