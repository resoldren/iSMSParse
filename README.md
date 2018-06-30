# iSMSParse
iSMSParse is a set of perl scripts that can parse your iPhone SMS database and export the call information in CSV format.
# Installation
1. Download the perl scripts
# Usage
```perl iSMSDumpReal.pl --out C:\Where\To\Store\Output --map ManualContactLookup.txt```
# iPhone Database Fields
## Table: message
The message table contains information about each individual message

Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER||#|Row in database
guid|TEXT||GUID|GUID for message
text|TEXT||A/''|The text of the message, if any
replace|INTEGER|0|0|Usage not yet identified
service_center|TEXT||NULL|Usage not yet identified
handle_id|INTEGER|0|0/+|TODO - move this to its own section below<ul><li>Sharing location with others: 0 (item_type = 4, share_direction = 0)</li><li>Sharing location with you: handle_id of the person sharing with you (item_type = 4, share_direction = 1)</li><li>Leaving chat: handle_id of the person leaving the chat, or zero if it was a non-iPhone user (item_type = 3)</li></ul>
subject|TEXT||''/A|If the message was sent via email, this is the subject of the email
country|TEXT||NULL/us|Currently NULL. Older messages (of mine) contain 'us'
attributedBody|BLOB||BIN|TODO - more complicated version of text field
version|INTEGER|0|10/1|Currently 10. Older messages contain 1.
type|INTEGER|0|0/1|Usage not yet identified
service|TEXT||iMessage/SMS|Service used to send message
account|TEXT|
account_guid|TEXT|
error|INTEGER|0
date|INTEGER|
date_read|INTEGER|
date_delivered|INTEGER|
is_delivered|INTEGER|0
is_finished|INTEGER|0
is_emote|INTEGER|0
is_from_me|INTEGER|0
is_empty|INTEGER|0
is_delayed|INTEGER|0
is_auto_reply|INTEGER|0
is_prepared|INTEGER|0
is_read|INTEGER|0
is_system_message|INTEGER|0
is_sent|INTEGER|0
has_dd_results|INTEGER|0
is_service_message|INTEGER|0
is_forward|INTEGER|0
was_downgraded|INTEGER|0
is_archive|INTEGER|0
cache_has_attachments|INTEGER|0
cache_roomnames|TEXT|0|
was_data_detected|INTEGER|0
was_deduplicated|INTEGER|0
is_audio_message|INTEGER|0
is_played|INTEGER|0
date_played|INTEGER|
item_type|INTEGER|0
other_handle|INTEGER|-1
group_title|TEXT|
group_action_type|INTEGER|0
share_status|INTEGER|
share_direction|INTEGER|
is_expirable|INTEGER|0
expire_state|INTEGER|0
message_action_type|INTEGER|0
message_source|INTEGER|0
associated_message_guid|STRING|NULL
balloon_bundle_id|STRING|NULL
payload_data|BLOB|
associated_message_type|INTEGER|0
expressive_send_style_id|STRING|NULL
associated_message_range_location|INTEGER|0
associated_message_range_length|INTEGER|0
time_expressive_send_played|INTEGER|0
message_summary_info|BLOB|NULL|BLOB/NULL|Text and Unicode text seen. Handwritten message class info seen. Usage not yet identified
ck_sync_state|INTEGER|0|0|Usage not yet identified
ck_record_id|TEXT|NULL|NULL/''|Usage not yet identified
ck_record_change_tag|TEXT|NULL|NULL/''|Usage not yet identified
destination_caller_id|TEXT|NULL|NULL/MY#|Sometimes this field contains your phone number. Pattern not yet identified
sr_ck_sync_state|INTEGER|0|0|Usage not yet identified
sr_ck_record_id|TEXT|NULL|NULL/''|Usage not yet identified
sr_ck_record_change_tag|TEXT|NULL|NULL/''|Usage not yet identified

Notes to add:
* CR\LF style in text
* Unicode storage in text

## Table: attachment
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
created_date|INTEGER|0|0|0
start_date|INTEGER|0|0|0
filename|TEXT|0||0
uti|TEXT|0||0
mime_type|TEXT|0||0
transfer_state|INTEGER|0|0|0
is_outgoing|INTEGER|0|0|0
user_info|BLOB|0||0
transfer_name|TEXT|0||0
total_bytes|INTEGER|0|-1|0
is_sticker|INTEGER|0|0|0
sticker_user_info|BLOB|0||0
attribution_info|BLOB|0||0
hide_attachment|INTEGER|0|0|0
ck_sync_state|INTEGER|0|0|0
ck_server_change_token_blob|BLOB|0|NULL|0
ck_record_id|TEXT|0|NULL|0
original_guid|TEXT|0||0
sr_ck_record_id|TEXT|0|NULL|0
sr_ck_sync_state|INTEGER|0|0|0
sr_ck_server_change_token_blob|BLOB|0|NULL|0
## Table: chat
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
style|INTEGER|0||0
state|INTEGER|0||0
account_id|TEXT|0||0
properties|BLOB|0||0
chat_identifier|TEXT|0||0
service_name|TEXT|0||0
room_name|TEXT|0||0
account_login|TEXT|0||0
is_archived|INTEGER|0|0|0
last_addressed_handle|TEXT|0||0
display_name|TEXT|0||0
group_id|TEXT|0||0
is_filtered|INTEGER|0|0|0
successful_query|INTEGER|0|1|0
engram_id|TEXT|0||0
server_change_token|TEXT|0||0
ck_sync_state|INTEGER|0|0|0
last_read_message_timestamp|INTEGER|0|0|0
ck_record_system_property_blob|BLOB|0||0
original_group_id|TEXT|0|NULL|0
sr_server_change_token|TEXT|0||0
sr_ck_sync_state|INTEGER|0|0|0
cloudkit_record_id|TEXT|0|NULL|0
sr_cloudkit_record_id|TEXT|0|NULL|0
## Table: chat_handle_join
Name|Type|Default|Values|Description
-|-|-|-|-
chat_id|INTEGER|0||0
handle_id|INTEGER|0||0
## Table: chat_message_join
Name|Type|Default|Values|Description
-|-|-|-|-
chat_id|INTEGER|0||1
message_id|INTEGER|0||2
message_date|INTEGER|0|0|0
## Table: deleted messages
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
## Table: handle
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
id|TEXT|1||0
country|TEXT|0||0
service|TEXT|1||0
uncanonicalized_id|TEXT|0||0
## Table: kvtable
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
key|TEXT|1||0
value|BLOB|1||0
## Table: message_attachment_join
Name|Type|Default|Values|Description
-|-|-|-|-
message_id|INTEGER|0||0
attachment_id|INTEGER|0||0
## Table: message_processing_task
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
task_flags|INTEGER|1||0
## Table: sqlite_sequence
Name|Type|Default|Values|Description
-|-|-|-|-
name||0||0
seq||0||0
## Table: sync_deleted_attachments
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
recordID|TEXT|0||0
## Table: sync_deleted_chats
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
recordID|TEXT|0||0
timestamp|INTEGER|0||0
## Table: sync_deleted_messages
Name|Type|Default|Values|Description
-|-|-|-|-
ROWID|INTEGER|0||1
guid|TEXT|1||0
recordID|TEXT|0||0
