# iSMSDumpReal.pl

use iSMSParse;
use strict;
use File::Copy;
use File::Path;
use Getopt::Long;
use Path::Class;
use Data::Dumper;
use POSIX qw(strftime);

##############################################################################
####  OPTIONS
##############################################################################

my $limit = 0; # 0 means 'all'
my $mode = 'chats'; # validate|signatures|buckets|handles|chats
my $manualPhoneMapPath;
my $outputDirectory;
my $only;
my $onlyMessage;

GetOptions (
	'limit|max|m=i' => \$limit,
	'operation|o|mode=s' => sub {
		my ($opt_name, $opt_value) = @_;
		if ($opt_value eq 'buckets' || $opt_value eq 'b') {
			$mode = 'buckets';
		} elsif ($opt_value eq 'signatures' || $opt_value eq 's') {
			$mode = 'signatures';
		} elsif ($opt_value eq 'validate' || $opt_value eq 'v') {
			$mode = 'validate';
		} elsif ($opt_value eq 'handles' || $opt_value eq 'h') {
			$mode = 'handles';
		} elsif ($opt_value eq 'chats' || $opt_value eq 'c') {
			$mode = 'chats';
		} else {
			die "invalid operational mode: $opt_value";
		}
	},
	'map=s' => \$manualPhoneMapPath,
	'output|d=s' => \$outputDirectory,
	'only=s' => \$only,
	'onlyMessage=s' => \$onlyMessage,
) || die;

die 'Output directory not specified for chats mode' if $mode eq 'chats' && !defined($outputDirectory);
die 'Invalid output directory: ' . $outputDirectory if $mode eq 'chats' && !-d $outputDirectory;

##############################################################################
####  MAIN
##############################################################################

my @backups = iSMSParse::Backup::list() or die "No backups found";
my $backup = $backups[0];

print "contacts\n", $backup->contacts->databaseCommand, "\n";

my $manualPhoneMap = $manualPhoneMapPath ? parseManualPhoneMap($manualPhoneMapPath) : {};

if ($mode eq 'validate') {
	exit validateMessages(reverse $backup->sms->messages) ? 0 : 1;
} elsif ($mode eq 'signatures') {
	printSignatures(reverse $backup->sms->messages);
} elsif ($mode eq 'buckets') {
	my $count = 0;
	foreach my $message (reverse $backup->sms->messages) {
		last if $limit && $count++ >= $limit;
		my $bucket = pickBucket($message);
		# next unless $message->other_handle > 0;
		# print $message->ROWID, " --> ", $bucket, ' (', $message->handle_id, ',', $message->other_handle, ')', "\n" if defined $bucket;
		print $message->ROWID, " --> ", $bucket, "\n" if defined $bucket;
	}
} elsif ($mode eq 'handles') {
	my $r = makeContactLookup($backup->contacts);
	# print join("\n", map { "$_ => $r->{$_}" } keys %{$r}), "\n";
	# validateHandles($backup, $manualPhoneMap);
} elsif ($mode eq 'chats') {
	my $contactLookupRef = makeContactLookup($backup->contacts);
	my $handleLookupRef = makeHandleLookup($backup->sms, $contactLookupRef, $manualPhoneMap);
	doChats($backup, $handleLookupRef, $outputDirectory, $only);
}

exit 0;

##############################################################################
####  VALIDATION
##############################################################################

BEGIN {
	my %validation = (
		'ROWID' => ['%ANY%'],
		'guid' => ['%SET%'],
		'text' => ['%ANY%'], # ['%SET%'] # Can be NULL...when?
		'replace' => [0],
		'service_center' => [undef],
		'handle_id' => [0, '%POSITIVE%'], # Can be zero when is_from_me=1
		'subject' => ['%ANY%'],
		'country' => [undef, 'us'],
		'attributedBody' => ['%ANY%'],
		'version' => [1, 10],
		'type' => [0, 1],
		'service' => ['SMS', 'iMessage'],
		'account' => ['%ANY%'],
		'account_guid' => ['%ANY%'],
		'error' => ['%ANY%'],
		'date' => ['%SET%'],
		'date_read' => ['%ANY%'],
		'date_delivered' => ['%ANY%'],
		'is_delivered' => [0, 1],
		'is_finished' => [1],
		'is_emote' => [0],
		'is_from_me' => [0, 1],
		'is_empty' => [0],
		'is_delayed' => [0],
		'is_auto_reply' => [0],
		'is_prepared' => [0, 1],
		'is_read' => [0, 1],
		'is_system_message' => [0],
		'is_sent' => [0, 1],
		'has_dd_results' => [0, 1],
		'is_service_message' => [0],
		'is_forward' => [0],
		'was_downgraded' => [0, 1],
		'is_archive' => [0],
		'cache_has_attachments' => [0, 1],
		'cache_roomnames' => ['%ANY%'],
		'was_data_detected' => [0, 1],
		'was_deduplicated' => [0, 1],
		'is_audio_message' => [0, 1],
		'is_played' => [0, 1],
		'date_played' => ['%ANY%'],
		'item_type' => [0, 1, 3, 4],
		'other_handle' => [-1, 0, '%POSITIVE%'],
		'group_title' => [undef],
		'group_action_type' => [0],
		'share_status' => [undef, 0, 1],
		'share_direction' => [undef, 0],
		'is_expirable' => [0, 1],
		'expire_state' => [0],
		'message_action_type' => [0],
		'message_source' => [0],
		'associated_message_guid' => [undef, '%REGEX:^(p:[012]/|bp:|)[0-9A-F]{8,8}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{12,12}$%'],
		'balloon_bundle_id' => [undef,
			'com.apple.DigitalTouchBalloonProvider',
			'com.apple.Handwriting.HandwritingProvider',
			'com.apple.messages.MSMessageExtensionBalloonPlugin:EWFNLB79LQ:com.gamerdelights.gamepigeon.ext',
			'com.apple.messages.URLBalloonProvider'
		],
		'payload_data' => ['%ANY%'],
		'associated_message_type' => [0, 2, 3, 2000, 2001, 2003, 2004],
		'expressive_send_style_id' => [undef],
		'associated_message_range_location' => ['%ANY%'],
		'associated_message_range_length' => ['%ANY%'],
		'time_expressive_send_played' => ['%ANY%'],
		'message_summary_info' => ['%ANY%'],
		'ck_sync_state' => [0],
		'ck_record_id' => [undef, ''],
		'ck_record_change_tag' => [undef, ''],
		'destination_caller_id' => ['', '%ANY%'], # ['', '%MYPHONE%'] # HOW TO DO
		'sr_ck_sync_state' => [0],
		'sr_ck_record_id' => [undef, ''],
		'sr_ck_record_change_tag' => [undef, ''],
	);

	sub validateMessages {
		my $valid = 1;
		my $count = 0;
		foreach my $message (@_) {
			$valid = 0 if !validateMessage($message);
			last if $limit && ++$count >= $limit;
		}
		return $valid;
	}

	sub validateMessage {
		my $message = shift;
		my @errors = ();

		foreach my $key ($message->keys) {
			# print "$key = ", $message->$key, " ";
			if (!exists($validation{$key})) {
				push @errors, "Unknown key in message: $key";
			} elsif (!validateKey($message->$key, $validation{$key})) {
				push @errors, "Invalid value for $key: \"" . $message->$key . "\"";
			} else {
				# key validated fine
			}
		}

		if (@errors > 0) {
			print $message->ROWID, ": ", join(", ", @errors), "\n";
			return 0;
		} else {
			return 1;
		}
	}


	sub validateKey {
		my $val = shift;
		my $valid = shift;
		# print "Value: ", valString($val), "\n";
		foreach my $validValue (@{$valid}) {
			# print "  vs ", valString($validValue), " = ";
			if (validateValue($val, $validValue)) {
				# print "VALID\n";
				return 1;
			} else {
				# print "INVALID\n";
			}
		}
		return 0;
	}

	sub validateValue {
		my $val = shift;
		my $valid = shift;
		return 1 if $valid eq '%ANY%';
		return 1 if !defined($valid) && !defined($val);
		return 0 if !defined($val);
		return 1 if $valid eq '%SET%' && $val ne '';
		return 1 if $valid eq '%POSITIVE%' && $val > 0;
		return 1 if $valid =~ '^%REGEX:(.*)%$' && $val =~ /$1/;
		return 1 if $valid eq $val;
		return 0;
	}

	sub valString {
		my $val = shift;
		return defined($val) ? $val : "NULL";
	}
}

##############################################################################
####  SIGNATURES
##############################################################################

BEGIN {

	my %patterns = (
		'ROWID' => \&ignore,
		'guid' => \&ignore,
		'text' => \&existsFunction,
		'replace' => \&normal,
		'service_center' => \&normal,
		'handle_id' => \&existsFunction,
		'subject' => \&ignore,
		'country' => \&ignore,
		'attributedBody' => \&ignore,
		'version' => \&ignore,
		'type' => \&ignore, # This one drives me crazy, but I haven't been able to figure it out
		'service' => \&ignore,
		'account' => \&ignore,
		'account_guid' => \&ignore,
		'error' => \&nonzero,
		'date' => \&existsFunction,
		'date_read' => \&existsFunction,
		'date_delivered' => \&existsFunction,
		'is_delivered' => \&ignore,
		'is_finished' => \&normal,
		'is_emote' => \&normal,
		'is_from_me' => \&ignore,
		'is_empty' => \&normal,
		'is_delayed' => \&normal,
		'is_auto_reply' => \&normal,
		'is_prepared' => \&ignore,
		'is_read' => \&ignore,
		'is_system_message' => \&normal,
		'is_sent' => \&ignore,
		'has_dd_results' => \&ignore,
		'is_service_message' => \&normal,
		'is_forward' => \&normal,
		'was_downgraded' => \&ignore,
		'is_archive' => \&normal,
		'cache_has_attachments' => \&ignore,
		'cache_roomnames' => \&ignore,
		'was_data_detected' => \&ignore,
		'was_deduplicated' => \&ignore,
		'is_audio_message' => \&normal,
		'is_played' => \&normal,
		'date_played' => \&ignore,
		'item_type' => \&normal,
		'other_handle' => \&positive,
		'group_title' => \&existsFunction,
		'group_action_type' => \&normal,
		'share_status' => \&number,
		'share_direction' => \&number,
		'is_expirable' => \&ignore,
		'expire_state' => \&normal,
		'message_action_type' => \&normal,
		'message_source' => \&normal,
		'associated_message_guid' => \&existsFunction, # SHOULD BE FANCIER: [undef, '%REGEX:^(p:[012]/|bp:|)[0-9A-F]{8,8}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{12,12}$%'],
		'balloon_bundle_id' => \&normal,
		'payload_data' => \&existsFunction,
		'associated_message_type' => \&nonzero,
		'expressive_send_style_id' => \&existsFunction, # MAYBE SHOULD BE NORMAL
		'associated_message_range_location' => \&ignore,
		'associated_message_range_length' => \&nonzero,
		'time_expressive_send_played' => \&existsFunction,
		'message_summary_info' => \&existsFunction,
		'ck_sync_state' => \&normal,
		'ck_record_id' => \&existsFunction,
		'ck_record_change_tag' => \&existsFunction,
		'destination_caller_id' => \&ignore,
		'sr_ck_sync_state' => \&normal,
		'sr_ck_record_id' => \&existsFunction,
		'sr_ck_record_change_tag' => \&existsFunction,
	);

	sub ignore {
		return '';
	}

	sub existsFunction {
		my $val = shift;
		return (defined($val) && $val ne '') ? '1' : '0';
	}

	sub normal {
		my $val = shift;
		return defined($val) ? $val : 'UNDEF';
	}

	sub nonzero {
		my $val = shift;
		return (defined($val) && $val != 0) ? 'NZ' : '0';
	}

	sub number {
		my $val = shift;
		return defined($val) ? $val : 0;
	}

	sub positive {
		my $val = shift;
		return defined($val) ? ($val > 0) : 0;
	}

	sub formatSig {
		my $sig = shift;
		my $index = @_ ? shift : 1;
		return join(",", map { (split(/=/, $_))[$index] } split(/,/, $sig));
	}

	sub printSignatures {
		my $sigs = makeSignatures(@_);
		my $sigCount = scalar(keys %{$sigs});
		print STDERR "SIGNATURES: $sigCount\n";
		return if !$sigCount;
		print 'COUNT,MIN,MAX,', formatSig((keys %{$sigs})[0], 0), "\n";
		foreach my $sig (keys %{$sigs}) {
			print
				${$sigs}{$sig}->{count}, ',',
				${$sigs}{$sig}->{minRow}, ',',
				${$sigs}{$sig}->{maxRow}, ',',
				formatSig($sig), "\n";
		}
	}

	sub makeSignatures {
		my %sigs;
		my $count = 0;
		foreach my $message (@_) {
			my $sig = makeSignature($message);
			# print $sig, "\n\n";
			my $rowId = $message->ROWID;
			if (!exists($sigs{$sig})) {
				$sigs{$sig} = { 
					count => 1,
					minRow => $rowId,
					maxRow => $rowId,
					allRows => [$rowId]
				};
			} else {
				$sigs{$sig}->{count}++;
				push @{$sigs{$sig}->{allRows}}, $rowId;
				$sigs{$sig}->{minRow} = $rowId if $rowId < $sigs{$sig}->{minRow};
				$sigs{$sig}->{maxRow} = $rowId if $rowId > $sigs{$sig}->{maxRow};
			}
			last if $limit && ++$count >= $limit;
		}
		return \%sigs;
	}

	### IDEA
	# identify if one of the 'keys' in a signature always has the same value.  x=2,y set,z set   x=4,w set,z set, etc.
	sub makeSignature {
		my $message = shift;
		my $sig;
		my $first = 1;
		foreach my $key (sort (keys %patterns)) {
			my $val = $patterns{$key}->($message->$key);
			$sig .= "," if !$first; $first = 0;
			$sig .= "$key=$val";
		}
		return $sig;
	}
}

##############################################################################
####  BUCKETS
##############################################################################

BEGIN {

	my %emptyBucket = (
		'associated_message_guid' => [undef],
		'associated_message_range_length' => [undef, 0],
		'associated_message_type' => [undef, 0],
		'balloon_bundle_id' => [undef],
		'error' => [0],
		'handle_id' => [0, '%POSITIVE%'],
		'is_audio_message' => [0],
		'is_from_me' => [0, 1],
		'is_played' => [0],
		'item_type' => [0],
		'other_handle' => [0, -1],
		'payload_data' => [undef],
		'share_status' => [undef, 0],
		'share_direction' => [undef, 0],
		'subject' => [undef, ''],
		'text' => [undef, ''],
		'message_summary_info' => [undef],
	);

	sub makeBucket {
		my $overrides = shift;
		my %result = %emptyBucket;
		while (my($key, $val) = each %{$overrides}) {
			if (!defined($emptyBucket{$key})) {
				die "Unknown key in makeBucket overrides: $key";
			} else {
				$result{$key} = $val;
			}
		}
		return \%result;
	}

	my %msgTypes = (
		'standard' => makeBucket(
			{
				'text' => ['%ANY%'],
				'subject' => ['%ANY%'],
				'payload_data' => ['%ANY%'],
			}),
		'url_only' => makeBucket(
			{ # Treat same as sent
				'text' => ['%SET%'],
				'payload_data' => ['%SET%'],
				'balloon_bundle_id' => ['com.apple.messages.URLBalloonProvider'],
			}),
		'blooming_flower' => makeBucket(
			{
				'text' => [undef, ''],
				'is_played' => [0, 1],
				'payload_data' => ['%SET%'],
				'balloon_bundle_id' => ['com.apple.DigitalTouchBalloonProvider'],
			}),
		'audio_message' => makeBucket(
			{
				# deal with attachments?
				'text' => ['%SET%'],
				'is_audio_message' => [1],
			}),
		'emote' => makeBucket(
			{
				'text' => ['%SET%'],
				'associated_message_guid' => [undef, '%REGEX:^(p:[012]/|bp:)[0-9A-F]{8,8}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{12,12}$%'],
				'associated_message_type' => [2000, 2001, 2003, 2004],
				'associated_message_range_length' => ['%POSITIVE%', -1],
				'message_summary_info' => ['%SET%'],
			}),
		'game_pigeon' => makeBucket(
			{
				'text' => ['%SET%'],
				'payload_data' => ['%SET%'],
				'balloon_bundle_id' => ['com.apple.messages.MSMessageExtensionBalloonPlugin:EWFNLB79LQ:com.gamerdelights.gamepigeon.ext'],
				'associated_message_guid' => [undef, '%REGEX:^[0-9A-F]{8,8}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{4,4}-[0-9A-F]{12,12}$%'],
				'associated_message_type' => [2, 3],
				'associated_message_range_length' => ['%ANY%'],
			}),
		'handwriting' => makeBucket(
			{
				'payload_data' => ['%SET%'],
				'balloon_bundle_id' => ['com.apple.Handwriting.HandwritingProvider'],
				'is_played' => [0, 1],
			}),
		'error_standard' => makeBucket(
			{
				'text' => ['%SET%'],
				'error' => ['%POSITIVE%'],
				'handle_id' => [0, '%POSITIVE%'], # was this needed? Is it ever non zero?
			}),
		'join_chat' => makeBucket(
			{
				# TODO - maybe deal with handle, other_handle
				'item_type' => [1],
				'handle_id' => ['%POSITIVE%'],  # Person doing the adding
				'other_handle' => ['%POSITIVE%'], # Person added
			}),
		'leave_chat' => makeBucket(
			{
				# TODO - maybe deal with handle, other_handle
				'item_type' => [3],
				'handle_id' => [0, '%POSITIVE%'], # When this is 0, I believe a non-iPhone left chat
				'other_handle' => [0],
			}),
		'sharing_location_with_me' => makeBucket(
			{
				'item_type' => [4],
				'share_status' => [0, 1],
				'share_direction' => [1],
				'handle_id' => ['%POSITIVE%'],
				'other_handle' => [0],
			}),
		'sharing_location_with_others' => makeBucket(
			{
				'item_type' => [4],
				'share_status' => [0, 1],
				'share_direction' => [0],
				'handle_id' => [0],
				'other_handle' => ['%POSITIVE%'],
			}),
	);

	sub pickBucket {
		my $message = shift;
		my $bucket;
		while (my($bucketName, $bucketRules) = each %msgTypes) {
			# Not sure why this was needed, but without it, bucketRules
			# were 'emptyRules' MINUS any key that tried to be overridden
			keys(%{$bucketRules});
			if (messageInBucket($message, $bucketRules)) {
				if (defined($bucket)) {
					warn "Message " . $message->ROWID . " in multiple buckets: $bucket and $bucketName";
				} else {
					$bucket = $bucketName;
				}
			}
		}
		if (!defined($bucket)) {
			warn "Message " . $message->ROWID . " not in any bucket";
			foreach my $key (sort keys(%emptyBucket)) {
				print "$key = ", defined($message->$key) ? $message->$key : 'undef', "\n";
			}
		}
		return $bucket;
	}

	sub messageInBucket {
		my $message = shift;
		my $bucketRules = shift;
		while (my($ruleKey, $ruleValues) = each %{$bucketRules}) {
			return 0 if !validateKey($message->$ruleKey, $ruleValues);
		}
		return 1;
	}

}

##############################################################################
####  CHATS
##############################################################################

sub doChats {
	my ($backup, $handleLookupRef, $outputDirectory, $only) = @_;
	my $count = 0;
	my %chatInfos;

	backupDatabases($outputDirectory, $backup);

	# Read previous data and merge wtih incoming data
	foreach my $chat ($backup->sms->chats) {

		if (processChat($backup, $handleLookupRef, \%chatInfos, $chat, $only)) {
			last if $limit && ++$count >= $limit;
		}
	}

	# Write chats
	foreach my $chatInfoKey (keys %chatInfos) {
		my $chatInfo = $chatInfos{$chatInfoKey};
		print join(',', @{$chatInfo->{ids}}), ' modified: ', $chatInfo->{modified}, "\n";
		if ($chatInfo->{modified}) {
			my $chatPath = file($outputDirectory, $chatInfo->{file});
			writeChat($backup, $chatPath, $outputDirectory, $chatInfo->{rows}, $chatInfo->{columnNames});
		}
	}
}

sub processChat {
	my ($backup, $handleLookupRef, $chatsRef, $chat, $only) = @_;
	my $chatFile = makeChatPaths($chat, $handleLookupRef) || die "No chat file for chat " . $chat->ROWID;
	return 0 if defined($only) && $chatFile !~ /$only/;
	my $chatPath = file($outputDirectory, $chatFile);
	print STDERR $chat->ROWID, ':', $chatPath, "\n";

	# If this is the first chat that maps to this file
	if (!exists($chatsRef->{$chatFile})) {

		# Read existing data
		my %previousRows;
		my @columnNames;
		loadChat($chatPath, \%previousRows, \@columnNames);

		# Force column order if no order was seen
		@columnNames = ('guid', 'date', 'localdate', 'name', 'number', 'label', 'company', 'service', 'msgtype', 'direction', 'text', 'subject') if scalar(@columnNames) == 0;

		# Add new entry including existing data
		$chatsRef->{$chatFile} = {
			file => $chatFile,
			rows => \%previousRows,
			columnNames => \@columnNames,
		};
	}

	# Get reference to chat
	my $chatRef = $chatsRef->{$chatFile};

	# Add this chat's ROWID to the list of ids
	push @{$chatRef->{ids}}, $chat->ROWID;

	# Add the messages from this chat
	my @rows = ();
	foreach my $message ($chat->messages) {
		next if $onlyMessage && $message->ROWID != $onlyMessage;
		my $valuesRef = makeChatMessageValues($chat, $message, $handleLookupRef);
		if (defined $valuesRef) {
			if (updateRowValues($chatRef->{rows}, $valuesRef)) {
				$chatRef->{modified} = 1
			}
		}
	}
	return 1;
}

sub writeChat {
	my $backup = shift;
	my $chatPath = shift;
	my $outputDirectory = shift;
	my $rowsRef = shift;
	my $columnNamesRef = shift;

	if (!-d $chatPath->parent) {
		mkdir($chatPath->parent) || die "couldn't create " . $chatPath->parent;
	}

	my @rows;
	my @sortedRowKeys = sort { $rowsRef->{$a}->{date} <=> $rowsRef->{$b}->{date} } (keys %{$rowsRef});
	foreach (@sortedRowKeys) {
		my $row = $rowsRef->{$_};
		push @rows, makeRowFromValues($columnNamesRef, $row);
		if (exists($row->{'%ATTACHMENTS%'})) {
			foreach (@{$row->{'%ATTACHMENTS%'}}) {
				my $attachmentInfo = $_;
				my $dest = file($chatPath->parent, $attachmentInfo->{destpath});

				if (-f $dest) {
					# already copied
					next;
				}

				if (exists($attachmentInfo->{payload})) {
					if (open(my $PAYLOADOUT, ">$dest")) {
						syswrite $PAYLOADOUT, $attachmentInfo->{payload};
						close($PAYLOADOUT);
					} else {
						warn "Error writing payload file for $row->{'%GUID%'}";
					}
				} else {
					if (-f $attachmentInfo->{path}) {
						if (copy($attachmentInfo->{path}, $dest)) {
							if (!saveAttachment($outputDirectory, $backup, $attachmentInfo->{path})) {
								warn "Error backing up attachment " . $attachmentInfo->{path} . " for " . $row->{guid};
							}
							next;
						} else {
							warn "Error copying " . $attachmentInfo->{path} . " for " . $row->{guid};
						}
					} else {
						print STDERR 'Missing attachment for ', $row->{guid}, ":\n",
							'  source path: ', $attachmentInfo->{path}, "\n",
							'  dest path: ',  $attachmentInfo->{destpath}, "\n";
					}
					if (open(my $ATTACHOUT, ">$dest")) {
						print $ATTACHOUT 'Missing attachment: ', $attachmentInfo->{filename}, ' at ', $attachmentInfo->{path}, "\n";
						close($ATTACHOUT);
					} else {
						warn "Error writing placeholder file for missing attachment";
					}
				}

#				print $chatPath, '->', $row->{guid}, '->', 'HAS ATTACHMENT: ', $attachmentInfo->{filename}, '->', $attachmentInfo->{path}, '->', $dest, '->', (-f $attachmentInfo->{path} ? 'EXISTS' : 'MISSING'), "\n";
#				print $chatPath, '->', $row->{guid}, '->', 'HAS ATTACHMENT: ', ($present ? 'EXISTS' : 'MISSING'), "\n";
			}
		}
	}
	open(my $OUTH, ">$chatPath") || die "couldn't create " . $chatPath;
	syswrite $OUTH, join(',', @{$columnNamesRef});
	syswrite $OUTH, "\n";
	if (@rows) {
		syswrite $OUTH, join("\n", @rows);
		syswrite $OUTH, "\n";
	}
	close($OUTH);
}

sub loadChat {
	my $chatPath = shift;
	my $previousRowsRef = shift;
	my $columnNamesRef = shift;

	if (-f $chatPath) {
		my $headerRow = 1;
		my $fileObject = openFile($chatPath);
		while (my $valsRef = readCsvLine($fileObject)) {
			my @vals = @{$valsRef};
			if ($headerRow) {
				@{$columnNamesRef} = @vals;
				$headerRow = 0;
			} else {
				my %values;
				die "Unnamed columns in $chatPath" if scalar(@vals) > scalar(@{$columnNamesRef});
				for (my $i = 0; $i <= $#vals; ++$i) {
					my $key = $columnNamesRef->[$i];
					my $val = $vals[$i];
					$values{$key} = $val;
				}
				die "No guid for line $. in $chatPath" if !exists($values{guid}) || !$values{guid};
				my $guid = $values{guid};
				$previousRowsRef->{$guid} = \%values;
			}
		}
		closeFile($fileObject);
	}
}

sub updateRowValues {
	my $previousRowsRef = shift;
	my $valuesRef = shift;
	die if !exists($valuesRef->{guid});
	my $guid = $valuesRef->{guid};
	my $ans = 0;
	if (exists($previousRowsRef->{$guid})) {
		# overwrite
		my $destValues = $previousRowsRef->{$guid};
		while (my ($colName, $colVal) = each (%{$valuesRef})) {
			next if $colName eq 'guid';
			if ($colVal ne $destValues->{$colName}) {
				if ($colName !~ /^%.*%$/) {
					# print "MODIFIED $guid:$colName : 1\n";
					# print "  old: $destValues->{$colName}\n";
					# print "  new: $colVal\n";
					# map { print "old" . ord($_) . "\n" } split ('', $destValues->{$colName});
					# map { print "new" . ord($_) . "\n" } split ('', $colVal);
					$ans = 1;
				}
				$destValues->{$colName} = $colVal;
			}
		}
	} else {
		$previousRowsRef->{$guid} = $valuesRef;
		# print "MODIFIED $guid:NEW : 1\n";
		$ans = 1;
	}
	return $ans;
}

sub makeRowFromValues {
	my $columnNamesRef = shift;
	my $valuesRef = shift;
	my %usedColumnNames;
	my @columns;
	foreach my $colName (@{$columnNamesRef}) {
		if (exists($valuesRef->{$colName})) {
			push @columns, $valuesRef->{$colName};
			$usedColumnNames{$colName} = 1;
		} else {
			push @columns, '';
		}
	}
	while (my ($colName, $colVal) = each (%{$valuesRef})) {
		next if $usedColumnNames{$colName};
		next if $colName =~ /^%.*%$/;
		push @{$columnNamesRef}, $colName;
		push @columns, $colVal;
	}
	return join(',', map { csvEscape($_) } @columns);
}

sub makeChatMessageValues {
	my ($chat, $message, $handleLookupRef) = @_;
	my $valuesRef = makeChatMessageValuesBase($chat, $message);
	my $bucket = pickBucket($message);
	$valuesRef->{direction} = $message->is_from_me ? 'SENT' : 'RCVD';
	my $handleMode = 1;
	my $handleText = 0;
	if ($bucket eq 'standard' || $bucket eq 'url_only') {
		$valuesRef->{msgtype} = 'TEXT';
		$handleText = 1;
		$valuesRef->{subject} = $message->subject;
	} elsif ($bucket eq 'blooming_flower') {
		$valuesRef->{msgtype} = 'FLOWER';
	} elsif ($bucket eq 'audio_message') {
		$valuesRef->{msgtype} = 'AUDIO';
		$handleText = 1;
	} elsif ($bucket eq 'emote') {
		$valuesRef->{msgtype} = 'EMOTE';
		$handleText = 1;
	} elsif ($bucket eq 'game_pigeon') {
		$valuesRef->{msgtype} = 'GAME PIGEON';
		$valuesRef->{text} = '';
	} elsif ($bucket eq 'handwriting') {
		$valuesRef->{msgtype} = 'HANDWRITING';
	} elsif ($bucket eq 'error_standard') {
		$valuesRef->{msgtype} = 'ERR';
		$handleText = 1;
	} elsif ($bucket eq 'join_chat') {
		$valuesRef->{msgtype} = 'JOIN';
		$handleMode = 2; # person joining is in other_handle
		my $adderName = getHandleName($message->handle_id, $handleLookupRef); # person doing the adding is in handle_id
		if ($adderName) {
			$valuesRef->{text} = "Added by $adderName";
		}
	} elsif ($bucket eq 'leave_chat') {
		$valuesRef->{msgtype} = 'LEAVE';
		if ($message->handle_id == 0) {
			$valuesRef->{name} = "non iPhone user";
			$handleMode = 0;
		} else {
			$handleMode = 1; # person leaving is in handle_id
		}
	} elsif ($bucket eq 'sharing_location_with_me') {
		$valuesRef->{direction} = 'RCVD';
		$valuesRef->{msgtype} = (($message->share_status == 1) ? 'HIDE LOC' : 'SHARE LOC');
		$handleMode = 1; # Sharer is in handle_id
	} elsif ($bucket eq 'sharing_location_with_others') {
		$valuesRef->{direction} = 'SENT';
		$valuesRef->{msgtype} = (($message->share_status == 1) ? 'HIDE LOC' : 'SHARE LOC');
		$handleMode = 2; # Sharer is in other_handle
	} else {
		warn $bucket;
		$valuesRef->{text} = $bucket;
		$valuesRef->{subject} = 'TODO';
		$handleMode = 0;
	}
	# Deal with payload before text, so that it can show up in attachments
	my $data = $message->payload_data;
	if (defined($data)) {
		my $destPath = $message->guid . '.payload';
	    push @{$valuesRef->{'%ATTACHMENTS%'}}, { destpath => $destPath, payload => $data, skipmarker => 1 };
	}

	# Processing text can mark attachments as used, meaning that their path
	# was included in the object placement marker text, and they do not need
	# to have their path name added to an attachmenet column
	my $attachmentColumns = 0;
	if ($handleText) {
		$valuesRef->{text} = cleanMessageText($message->text, $message->ROWID, $valuesRef->{'%ATTACHMENTS%'}, $message->ROWID);
	}

	# Add attachment name to attachment column for attachments not
	# mentioned in text column.
	my $attachmentCount = $valuesRef->{'%ATTACHMENTS%'} ? scalar(@{$valuesRef->{'%ATTACHMENTS%'}}) : 0;
	for my $attachmentRef (@{$valuesRef->{'%ATTACHMENTS%'}}) {
		if (!$attachmentRef->{used}) {
			$attachmentColumns++;
		    my $attachmentKey = 'attachment' . $attachmentColumns;
		    $valuesRef->{$attachmentKey} = $attachmentRef->{destpath};
		    if (!$attachmentRef->{skipmarker}) {
			    print STDERR 'Missing attachment marker for ', $message->ROWID, '/', $message->guid, ":\n",
			    	'  destpath: ', $attachmentRef->{destpath}, "\n";
			}
		}
	}

	if ($handleMode == 1) {
		addHandleValues($valuesRef, $message->handle_id, $handleLookupRef);
	} elsif ($handleMode == 2) {
		addHandleValues($valuesRef, $message->other_handle, $handleLookupRef);
	}
	return wantarray ? %{$valuesRef} : $valuesRef;
}

#my $countX = 0;
sub cleanMessageText {
	my $t = shift;
	my $messageRowId = shift;
	my $attachmentsRef = shift;
	my $rowid = shift; # TODO - remove
	my $decoded = $t;
	utf8::decode($decoded);
	my $clean = join('', map { cleanUnicode($_, $messageRowId, $attachmentsRef) } split('', $decoded));
#	if ($clean ne $t) {
#		print $rowid, ': "', $t, '" "', $clean, '"', "\n";
#		die if $countX++ > 10;
#	}
	return $clean;
}

sub cleanUnicode {
	my $ch = shift;
	my $messageRowId = shift;
	my $attachmentsRef = shift;
	my $code = ord($ch);
	my $chEnc = $ch;
	utf8::encode($chEnc);	
	if ($code != 65532) {
		return $chEnc;
	}
	if ($code < 128) {
		return $chEnc;
	}
	my @parts = ('u', $code, $chEnc);
	my $name = nameForUnicode($code);
	push @parts, $name if $name;
	if ($code == 65532) {
		my $found = 0;
		if ($attachmentsRef) {
			for my $attachmentRef (@{$attachmentsRef}) {
				if (!$attachmentRef->{used} && !$attachmentRef->{skipmarker}) {
					$attachmentRef->{used} = 1;
					push @parts, $attachmentRef->{destpath};
					$found = 1;
					last;
				}
			}
		}
		warn "$messageRowId: No attachment for marker" if !$found;
	}
	return '<' . join(':', @parts) . '>';
}

sub nameForUnicode {
	my $code = shift;
	return "OBJECT REPLACEMENT CHARACTER" if $code == 65532;
	return undef;
	return "Latin Small Letter N With Tilde" if $code == 241;
	return "Thumbs Up Sign" if $code == 128077;
	return "Smiling Face With Smiling Eyes" if $code == 128522;
	warn 'No name for unicode ', $code, "\n";
	return undef;
}

sub getHandleName {
	my ($handleId, $handleLookupRef) = @_;
	if (defined($handleId) && $handleId > 0) {
		my $infoArray = $handleLookupRef->{$handleId};
		if (@{$infoArray} > 0) {
			my $infoRef = $infoArray->[0];
			return $infoRef->{name};
		}
	}
	return "";
}

sub addHandleValues {
	my ($valuesRef, $handleId, $handleLookupRef) = @_;
	if (defined($handleId) && $handleId > 0) {
		my $infoArray = $handleLookupRef->{$handleId};
		if (@{$infoArray} > 0) {
			my $infoRef = $infoArray->[0];
			$valuesRef->{name} = $infoRef->{name};
			$valuesRef->{number} = $infoRef->{lookup};
			$valuesRef->{company} = $infoRef->{company};
			$valuesRef->{label} = $infoRef->{label};
		}
	}
}

sub makeChatMessageValuesBase {
	my ($chat, $message) = @_;
	my $d = iSMSParse::fixDate($message->date);
	my %values = (
		'guid' => $message->guid,
		'date' => $d,
		'localdate' => iSMSParse::dateString($d),
		'service' => $message->service,
	);
    foreach my $attachment ($message->attachments) {
	    my $attachmentString;
#	    print STDERR "ATTACHMENT for ", $message->guid, ": ", $attachment->filename, ": ", $attachment->path, "\n";
		my $destPath = makeDestPath($message, $attachment->filename);
	    push @{$values{'%ATTACHMENTS%'}}, { filename => $attachment->filename, path => $attachment->path, destpath => $destPath };
	}

	return wantarray ? %values : \%values;
}

sub makeDestPath
{
	my $message = shift;
	my $sourcePath = shift;
	$sourcePath =~ /[\\\/]([^\\\/]+)$/;
	my $sourceFile = $1;
	$sourceFile = $message->guid if !$sourceFile;
	my $d = iSMSParse::fixDate($message->date);
	my $dateString = iSMSParse::dateString($d, '%Y%m%d-%H%M%S');
	my $path = $dateString . '-' . $sourceFile;
	return $path;
}

sub readCsvLine {
	my $fileObject = shift;
	my @ans;
	my $in = 0;
	my $val;
	while (my $line = readFileLine($fileObject)) {
		$line =~ s/([\n\r]+)$//;
		my $lineEnd = $1;
		while ($line) {
			if (!$in) {
				if ($line =~ s/^"//) {
					$in = 1;
					$val = '';
				} elsif ($line =~ s/^([^",]*)(,|$)//) {
					push @ans, $1;
				} else {
					die "Can't parse: $line";
				}
			} else {
				if ($line =~ s/^""//) {
					$val .= '"';
				} elsif ($line =~ s/^([^"]+)//) {
					$val .= $1;
				} elsif ($line =~ s/^"(,|$)//) {
					push @ans, $val;
					$in = 0;
				} else {
					die "Can't parse: $line";
				}
			}
		}
		if ($in) {
			$val .= $lineEnd;
		} else {
			return \@ans;
		}
	}
	die "mismatched quotes" if $in;
	return undef;
}

sub csvEscape {
	my $text = shift;
    my $needQuotes = 0;
    if ($text =~ /\n/) {
        $needQuotes = 1;
    }
    if ($text =~ /,/) {
        $needQuotes = 1;
    }
    if ($text =~ s/"/""/g) {
        $needQuotes = 1;
    }
    if ($needQuotes) {
        $text = '"' . $text . '"';
    }
    return $text;
}

sub getHandleNameFromInfo {
	my ($handleInfoRef) = @_;

	my $tmp = join(' or ', map { $_->{displayName} } @{$handleInfoRef});
	if ($tmp =~ /^1410/) {
		print Dumper($handleInfoRef);
		die $tmp 
	}
	return join(' or ', map { $_->{displayName} } @{$handleInfoRef});
}

sub makeChatPaths {
	my ($chat, $handleLookupRef) = @_;
    my %chatNames;
	foreach my $handle ($chat->handles) {
		my $infoRef = $handleLookupRef->{$handle->ROWID};
		my $name = getHandleNameFromInfo($infoRef);
        $chatNames{$name} = 1;
	}
	my $dirName = join(' - ', sort (keys %chatNames));
	if ($dirName =~ /^\s*$/) {
		warn "EMPTY: " . $chat->ROWID, "\n";
		foreach my $handle ($chat->handles) {
			my $infoRef = $handleLookupRef->{$handle->ROWID};
			my $name = getHandleNameFromInfo($infoRef);
			print '  ', $handle->id, ' ', Dumper($infoRef), ' "', $name, '"', "\n" if 1; # $handle->id =~ /eck/;
		}
		die;
	}
	my $fileName = 'messages.csv';
	return file($dirName, $fileName);
}

sub cleanHandleId {
	my $id = shift;
	$id =~ s/\+//g;
	$id =~ s/@/_/g;
	$id =~ s/ //g;
	return $id;
}

sub cleanHandleId_OLD {
	my $id = shift;
	$id =~ s/\+//g;
	$id =~ s/@/_/g;
	$id =~ s/ //g;
	return $id;
}

sub contactToChatName {
	my $contactRef = shift;
	if ($contactRef->{name}) {
		return $contactRef->{name};
	} elsif ($contactRef->{company}) {
		return $contactRef->{company};
	} else {
		die "Couldn't make name from contact: " . $contactRef->{ROWID};
	}
}

##############################################################################
####  CONTACTS
##############################################################################

sub makeContactLookup {
	my $db = shift;
	my %contactLookup;
	my %p;
	foreach my $person ($db->people) {
		my $personName = makePersonName($person);
		if (exists($p{$personName})) {
			warn "Duplicate person: $personName";
			$p{$personName} += 1;
			$personName = $personName . ' (' . $p{$personName} . ')';
		} else {
			$p{$personName} = 1;
		}
		foreach my $value ($person->values) {
			my $valueKey = $value->value;
			if ($value->property == 3) {
				# phone number, clean and continue
				$valueKey = cleanPhoneValue($valueKey);
			} elsif ($value->property == 4) {
				# email, continue
			} else {
				# other - ignore
				next;
			}
			# warn "Duplicate key: $valueKey for $personName vs " . join(", ", @{$contactLookup{$valueKey}})  if exists($contactLookup{$valueKey});
			my $labelName = $db->valueLabelName($value->label);
			push @{$contactLookup{$valueKey}},
				addContactDisplayName({
					'name' => $personName,
					'label' => $labelName,
					'company' => $person->Organization
				});
		}
	}
	foreach my $key (keys %contactLookup) {
		print $key, ' -> ', join(' or ', map { $_->{name} . ':' . $_->{label} . ':' . $_->{company} } @{$contactLookup{$key}}), "\n" if 1; # $key =~ /eck/;
	}
	return wantarray ? %contactLookup : \%contactLookup;
}

sub addContactDisplayName {
	my ($contactRef) = @_;
	$contactRef->{displayName} = $contactRef->{name} || $contactRef->{company};
	return $contactRef;
}

sub makePersonName {
	my $person = shift;
	my @parts;
	push @parts, $person->First if $person->First;
	push @parts, $person->Last if $person->Last;
	if (scalar(@parts)) {
		unshift @parts, $person->Prefix if $person->Prefix;
	}
	if (!scalar(@parts)) {
		push @parts, $person->Organization;
	}
	if (!scalar(@parts)) {
		die "Couldn't make a name for person " . $person->{ROWID};
	}
	return join(' ', @parts);
}

sub cleanPhoneValue {
	my $v = shift;
	$v =~ s/\D//g;
	$v =~ s/^1(\d{10,10})/\1/;
	$v;
}

##############################################################################
####  HANDLES
##############################################################################

sub makeHandleLookup {
	my ($sms, $contactLookupRef, $manualPhoneMap) = @_;
	my %handleLookup;
	foreach my $handle ($sms->handles) {
		$handleLookup{$handle->ROWID} = getHandleIdInfo($handle->id, $contactLookupRef, $manualPhoneMap);
	}
	foreach my $key (sort {$a <=> $b} (keys %handleLookup)) {
		print $key, ' -> ', join(' or ', map { join(':', $_->{name}, $_->{label}, $_->{company}, $_->{lookup}) } @{$handleLookup{$key}}), "\n" if 1; # $key =~ /eck/;
	}
	return wantarray ? %handleLookup : \%handleLookup;
}

sub getHandleIdInfo {
	my ($handleId, $contactLookupRef, $manualPhoneMap) = @_;
	my $lookup = makeHandleLookupKey($handleId);
	my $ansRef;
	if (defined($contactLookupRef->{$lookup})) {
		$ansRef = $contactLookupRef->{$lookup};
	} elsif (defined($manualPhoneMap->{$lookup})) {
		$ansRef = $manualPhoneMap->{$lookup};
	} else {
	    print "COULDN'T FIND $handleId ($lookup)\n" if 1; # $handleId =~ /eck/;
		$ansRef = [{
					'displayName' => $lookup,
				}];
	}
	foreach my $entry (@{$ansRef}) {
		$entry->{lookup} = $lookup;
	}
	return $ansRef;
}

sub makeHandleLookupKey {
	my $id = shift;
	return $id if $id =~ /@/ || $id =~ /[a-z]/i; # leave email addresses and anything with words alone
	# treat all others as phone numbers
	return cleanPhoneValue($id);
}

sub validateHandles {
	my ($backup, $manualPhoneMap) = @_;
	my $count = 0;
	foreach my $handle ($backup->sms->handles) {
		print $handle->id, "\n";
		last if $limit && ++$count >= $limit;
	}
}

sub parseManualPhoneMap {
	my ($path) = @_;
	my %phoneMap;

    open my $fh, '<', $path or die "Unable to open manual phone map: $path";
    while (<$fh>) {
        s/\s*$//;
        last if $_ eq "DONE";
        my @parts = split(/=/);
        my $valueKey = makeHandleLookupKey($parts[0]);
		push @{$phoneMap{$valueKey}},
			addContactDisplayName({
				'name' => (!$parts[1] && !$parts[3]) ? $valueKey : $parts[1],
				'label' => $parts[2],
				'company' => $parts[3],
			});
    }
    close $fh;

    # print Dumper(\%phoneMap);
	return \%phoneMap;
}

##############################################################################
####  SAVE BACKUPS
##############################################################################

sub outputRoot {
	my $outputDirectory = shift;
	return "$outputDirectory.backup";
}

sub databaseOutputDir {
	my $outputDirectory = shift;

	my $fmt = '%Y%m%d-%H%M%S';
	my ($sec,$min,$hour,$day,$month,$yr19,@rest) = localtime();
	my $date = strftime($fmt, $sec,$min,$hour,$day,$month,$yr19);

	return dir(outputRoot($outputDirectory), 'databases', $date);
}

sub attachmentOutputDir {
	my $outputDirectory = shift;

	return dir(outputRoot($outputDirectory), 'attachments');
}

sub saveDatabase {
	my $outputDirectory = shift;
	my $backup = shift;
	my $db = shift;

	return saveRecoveryFile(databaseOutputDir($outputDirectory), $backup->path, $db->path);
}

sub saveAttachment {
	my $outputDirectory = shift;
	my $backup = shift;
	my $attachmentPath = shift;

	return saveRecoveryFile(attachmentOutputDir($outputDirectory), $backup->path, $attachmentPath);
}

sub saveRecoveryFile {
	my ($dir, $backupRootPath, $filePath) = @_;
	my $backupSrc = $filePath;
	if ($filePath =~ s/^\Q$backupRootPath\E//) {
		$filePath =~ s/^[\\\/]*//;
		my $recoveryDest = file($dir, $filePath);
		File::Path::make_path($recoveryDest->parent) if !-d $recoveryDest->parent;
		print "copying $backupSrc to $recoveryDest\n";
		if (!copy($backupSrc, $recoveryDest)) {
			warn "error copying $backupSrc to $recoveryDest";
			return 0;
		} else {
			return 1;
		}
	} else {
		warn "couldn't find backup root ($backupRootPath) at start of file path: $filePath";
		return 0;
	}
}

sub backupDatabases {
	my ($outputDirectory) = @_;
	saveDatabase($outputDirectory, $backup, $backup->sms);
	saveDatabase($outputDirectory, $backup, $backup->contacts);
}

sub openFile {
	my $path = shift;
	my $FILEH;
	open($FILEH, $path) || die "Couldn't open $path";
	return {
		fileh => $FILEH,
		buffer => '',
		done => 0,
	};
}

sub closeFile {
	my $fileObject = shift;
	close($fileObject->{fileh});
}

sub readFileLine {
	my $file = shift;
	return undef if $file->{done};
	while (1) {
		if ($file->{buffer} =~ s/^([^\r\n]*[\r\n]+)//) {
			my $line = $1;
			return $line;
		} else {
			my $bytesRead = sysread($file->{fileh}, $file->{buffer}, 100, length($file->{buffer}));
			if (!$bytesRead) {
				$file->{done} = 1;
				return undef if !length($file->{buffer});
				return $file->{buffer};
			}
		}
	}
}