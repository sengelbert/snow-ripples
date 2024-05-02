-- Bootstrap the user, DB, schema, tables, etc, etc

create role if not exists xrpl_stream;

CREATE OR REPLACE USER xrpl_streamer
  PASSWORD = 'SuperStrongPass123!' --replace with your own super strong pwd
  MUST_CHANGE_PASSWORD = FALSE
  DEFAULT_ROLE = 'xrpl_stream'
  DEFAULT_WAREHOUSE = 'compute_wh';

grant role xrpl_stream to user xrpl_streamer;

CREATE DATABASE IF NOT EXISTS xrpl;
CREATE SCHEMA IF NOT EXISTS raw;

GRANT ALL ON DATABASE xrpl TO ROLE xrpl_stream;
GRANT ALL ON SCHEMA raw TO ROLE xrpl_stream;
GRANT ALL on WAREHOUSE compute_wh TO ROLE xrpl_stream;

CREATE OR REPLACE TABLE xrpl.raw.raw_xrpl (
  json_data VARIANT,
  host STRING,
  ledger STRING,
  loaded_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

GRANT ALL on TABLE XRPL.RAW.RAW_XRPL TO ROLE xrpl_stream;

-- get all entry types
-- select
--     distinct
--     repeated_json_data:LedgerEntryType::string as ledger_entry_type
-- from (
--     SELECT 
--         f.value AS repeated_json_data,
--         json_data
--     FROM 
--         XRPL.RAW.RAW_XRPL,
--         LATERAL FLATTEN(input => json_data:state) f
--         )
-- LEDGER_ENTRY_TYPE
-- AccountRoot
-- PayChannel
-- Ticket
-- Escrow
-- FeeSettings
-- SignerList
-- Offer
-- Check
-- DepositPreauth
-- LedgerHashes
-- AMM
-- NFTokenOffer
-- RippleState
-- NFTokenPage
-- DirectoryNode

-- get specific entry type examples
-- select
--     repeated_json_data
-- from (
--     SELECT 
--         f.value AS repeated_json_data,
--         json_data
--     FROM 
--         XRPL.RAW.RAW_XRPL,
--         LATERAL FLATTEN(input => json_data:state) f
--         )
-- where repeated_json_data:LedgerEntryType::string = 'PayChannel';
        
-- DROP TABLE XRPL.RAW.XRPL_ACCOUNT
CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_ACCOUNT_ROOT
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Balance::string as balance,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerCount as owner_count,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:Sequence as sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'AccountRoot';

-- select * from XRPL.RAW.XRPL_ACCOUNT_ROOT where ledger_index = '46332623'
-- select max(ledger_index) from XRPL.RAW.XRPL_ACCOUNT_ROOT
-- select count(*) from XRPL.RAW.XRPL_ACCOUNT_ROOT where ledger_index = '46597399'

-- select * from XRPL.RAW.RAW_XRPL order by loaded_at desc limit 1000 where file_name is null

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_DIRECTORY_NODE
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Flags as flags,
        repeated_json_data:Indexes::array as indexes,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:Owner::string as owner,
        repeated_json_data:TakerGetsCurrency::string as taker_gets_currency,
        repeated_json_data:TakerGetsIssuer::string as taker_gets_issuer,
        repeated_json_data:TakerPaysCurrency::string as taker_pays_currency,
        repeated_json_data:TakerPaysIssuer::string as taker_pays_issuer,
        repeated_json_data:RootIndex::string as root_index,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'DirectoryNode';

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_OFFER
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:BookDirectory::string as book_directory,
        repeated_json_data:BookNode as book_node,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:Sequence as sequence,
        repeated_json_data:TakerGets:currency::string as taker_gets_currency,
        repeated_json_data:TakerGets:issuer::string as taker_gets_issuer,
        repeated_json_data:TakerGets:value::string as taker_gets_value,
        repeated_json_data:TakerPays:currency::string as taker_pays_currency,
        repeated_json_data:TakerPays:issuer::string as taker_pays_issuer,
        repeated_json_data:TakerPays:value::string as taker_pays_value,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'Offer';

-- select * from XRPL.RAW.XRPL_OFFER where ledger_index = '46332623'

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_RIPPLE_STATE
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Balance:currency::string as balance_currency,
        repeated_json_data:Balance:issuer::string as balance_issuer,
        repeated_json_data:Balance:value::string as balance_value,
        repeated_json_data:Flags as flags,
        repeated_json_data:HighLimit:currency::string as high_limit_currency,
        repeated_json_data:HighLimit:issuer::string as high_limit_issuer,
        repeated_json_data:HighLimit:value::string as high_limit_value,
        repeated_json_data:HighNode::string as high_node,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:LowLimit:currency::string as low_limit_currency,
        repeated_json_data:LowLimit:issuer::string as low_limit_issuer,
        repeated_json_data:LowLimit:value::string as low_limit_value,
        repeated_json_data:LowNode::string as low_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'RippleState';

-- select * from XRPL.RAW.XRPL_OFFER where ledger_index = '46332623'

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_TICKET
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:TicketSequence as ticket_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'Ticket';

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_SIGNER_LIST
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:SignerEntries::array as signer_entries,
        repeated_json_data:SignerListID as signer_list_id,
        repeated_json_data:SignerQuorum as signer_quorum,
        repeated_json_data:TicketSequence as ticket_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'SignerList';

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_NFTOKEN_OFFER
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Amount::string as amount,
        repeated_json_data:Destination::string as destination,
        repeated_json_data:Expiration as expiration,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:NFTokenID::string as nftoken_id,
        repeated_json_data:NFTokenOfferNode::string as nftoken_offer_node,
        repeated_json_data:Owner::string as owner,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'NFTokenOffer';

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_NFTOKEN_PAGE
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:NFTokens::array as nftokens,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'NFTokenPage';


CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_AMM
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Asset:currency::string as asset_currency,
        repeated_json_data:Asset:issuer::string as asset_issuer,
        repeated_json_data:Asset2:currency::string as asset_2_currency,
        repeated_json_data:Asset2:issuer::string as asset_2_issuer,
        repeated_json_data:AuctionSlot:Account::string as auction_slot_account,
        repeated_json_data:AuctionSlot:DiscountedFee as auction_discounted_fee,
        repeated_json_data:AuctionSlot:Expiration as auction_expiration,
        repeated_json_data:AuctionSlot:Price:currency::string as auction_price_currency,
        repeated_json_data:AuctionSlot:Price:issuer::string as auction_price_issuer,
        repeated_json_data:AuctionSlot:Price:value::string as auction_price_value,
        repeated_json_data:Flags as flags,
        repeated_json_data:LPTokenBalance:currency::string as lptoken_balance_currency,
        repeated_json_data:LPTokenBalance:issuer::string as lptoken_balance_issuer,
        repeated_json_data:LPTokenBalance:value::string as lptoken_balance_value,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:TradingFee as trading_fee,
        repeated_json_data:VoteSlots::array as vote_slots,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'AMM';


CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_LEDGER_HASHES
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Flags as flags,
        repeated_json_data:Hashes::array as hashes,
        repeated_json_data:LastLedgerSequence::string as last_ledger_sequence,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'LedgerHashes';


CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_DEPOSIT_PREAUTH
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Authorize::string as authorize,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'DepositPreauth';

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_CHECK
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Destination::string as destination,
        repeated_json_data:DestinationNode::string as destination_node,
        repeated_json_data:DestinationTag as destination_tag,
        repeated_json_data:Expiration as expiration,
        repeated_json_data:Flags as flags,
        repeated_json_data:InvoiceID::string as invoice_id,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:SendMax::string as send_max,
        repeated_json_data:SendMax:currency::string as send_max_currency,
        repeated_json_data:SendMax:issuer::string as send_max_issuer,
        repeated_json_data:SendMax:value::string as send_max_value,
        repeated_json_data:Sequence as sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'Check';

CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_FEE_SETTINGS
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:BaseFee::string as base_fee,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:ReferenceFeeUnits as reference_fee_units,
        repeated_json_data:ReserveBase as reserve_base,
        repeated_json_data:ReserveIncrement as reserve_increment,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'FeeSettings';


CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_ESCROW
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Amount::string as amount,
        repeated_json_data:CancelAfter as cancel_after,
        repeated_json_data:Condition::string as condition,
        repeated_json_data:Destination::string as destination,
        repeated_json_data:DestinationNode::string as destination_node,
        repeated_json_data:FinishAfter as finish_after,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'Escrow';


CREATE OR REPLACE DYNAMIC TABLE XRPL.RAW.XRPL_PAY_CHANNEL
 TARGET_LAG = '1 minutes'
  WAREHOUSE = compute_wh
  AS
    select
        json_data:ledger_hash::string as ledger_hash,
        json_data:ledger_index as ledger_index,
        json_data:marker::string as marker,
        repeated_json_data:Account::string as account,
        repeated_json_data:Amount::string as amount,
        repeated_json_data:Balance::string as balance,
        repeated_json_data:CancelAfter as cancel_after,
        repeated_json_data:Destination::string as destination,
        repeated_json_data:DestinationNode::string as destination_node,
        repeated_json_data:Flags as flags,
        repeated_json_data:LedgerEntryType::string as ledger_entry_type,
        repeated_json_data:OwnerNode::string as owner_node,
        repeated_json_data:PreviousTxnID::string as previous_tranaction_id,
        repeated_json_data:PreviousTxnLgrSeq as previous_transaction_ledger_sequence,
        repeated_json_data:PublicKey::string as public_key,
        repeated_json_data:SettleDelay as settle_delay,
        repeated_json_data:index::string as index
    from (
        SELECT 
            f.value AS repeated_json_data,
            json_data
        FROM 
            XRPL.RAW.RAW_XRPL,
            LATERAL FLATTEN(input => json_data:state) f
        )
    where repeated_json_data:LedgerEntryType::string = 'PayChannel';