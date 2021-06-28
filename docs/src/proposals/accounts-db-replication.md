---
title: AccountsDB Replication for RPC Services
---

## Problem

Validators fall behind the network when bogged down by heavy RPC load. This
seems to be due to a combination of CPU load and lock contention caused by
serving RPC requests. The most expensive RPC requests involve account scans.

## Solution Overview

AccountsDB `replicas` that run separately from the main validator can be used to
offload account-scan requests. Replicas would only: request and pull account
updates from the validator, serve client account-state RPC requests, and manage
AccountsDb and AccountsBackgroundService clean + shrink.

The replica communicates to the main validator via a new RPC mechanism to fetch
metadata information about the replication and the accounts update from the validator.
The main validator supports only one replica node. A replica node can relay the
information to 1 or more other replicas forming a replication tree.

At the initial start of the replica node, it downloads the latest snapspshot
from a validator and constructs the bank and AccountsDb from it. After that, it queries
its main validator for new slots and request the validator to send the updated
accounts for that slot and update to its own AccountsDb.

The same RPC replication mechansim can be used between a replica to another replica.
This requires the replica to serve both the client and server in the replication model.

On the client RPC serving side, `JsonRpcAccountsService` is responsible for serving
the client RPC calls for accounts related information similar to the existing
`JsonRpcService`.

The replica will also take snapshots periodically so that it can start quickly after
a restart if the snapshot is not too old.

## Detailed Solution
The following sections provides more details of the design.

### Consistency Model
The AccountsDb information is replicated asynchronously from the main validator to the replica.
When a query against the replica's AccountsDb is made, the replica may not have the latest
information of the latest slot. In this regard, it will be eventually consistent. However, for
a particular slot, the information provided is consistent with the that of its peer validator
for commitment levels confirmed and finalized. For V1, we only support queries at these two
levels.

### Solana RPC Node
A new node named solana-rpc-node will be introduced whose main responsibility is to maintain
the AccountsDb replica. The RPC node or replica node is used interchangebly in this document.
It will be a separate exectuable from the validator.

The replica consists of the following major components.

The `ReplRpcUpdatedSlotsRequestor`, this service is responsible for peridically sending the
request `ReplRpcUpdatedSlotsRequest` to its peer validator or replica for the latest slots.
It specifies the latest slot (last_replicated_slot) for which the replica has already
fetched the accounts information for.

The `ReplRpcUpdatedSlotsServer`, this service is responsible for serving the
`ReplRpcUpdatedSlotsRequest` and sends the `ReplRpcUpdatedSlotsResponse` back to the requestor.
The response consists of a vector of new slots the validator knows of which is later than the
specified last_replicated_slot. This services also runs in the main validator.

The `ReplRpcAccountsRequestor`, this service is responsible for sending the request
`ReplRpcAccountsRequest` to its peer validator or replica for the `ReplAccountInfo` for a
slot for which it has not completed accounts db replication. The `ReplAccountInfo` contains
the `ReplAccountMeta`, Hash and the AccountData. The `ReplAccountMeta` contains info about
the existing `AccountMeta` in addition to the account data length in bytes.

The `ReplRpcAccountsServer`, this service is reponsible for serving the `ReplRpcAccountsRequest`
and sends `ReplRpcAccountsResponse` to the requestor. The response contains the count of the
ReplAccountInfo and the vector of ReplAccountInfo. This service runs both in the validator
and the replica relaying replication information. The server can stream the account information
from its AccountCache or from the storage if already flushed.

The `JsonRpcAccountsService`, this is the RPC service serving client requests for account
information. The existing JsonRpcService serves other client calls than AccountsDb ones.
The replica node only serves the AccountsDb calls.

The existing JsonRpcService requires `BankForks`, `OptimisticallyConfirmedBank` and
`BlockCommitmentCache` to load the Bank. The JsonRpcAccountsService will need to use 
information obtained from ReplRpcUpdatedSlotsResponse to construct the AccountsDb. Question,
do we still need the Bank in the replica?


The `AccountsBackgroundService`, this service also runs in the replica which is responsible
for taking snapshots periodically and shrinking the AccountsDb and doing accounts cleaning.
The existing code also uses BankForks which we need to keep in the replica.

### Compatibility Consideration

For protocol compatiblilty considerations, all the requests have the replication version which is
initially set to 1. Alternatively, we can use the validator's version. The RPC server side
shall check the request version and fail if it is not supported.

### Replication Setup
To limit adverse effects on the validator and the replica due to replication, they can be configured
with a list of replica nodes which can form a replication pair with it. And the replica node is
configured with the validator which can serve its requests.


### Fault Tolerance
The main responsibility of making sure the replication is tolerant of faults lies with the replica.
In case of request failures, the replica shall retry the requests. 


### Interface

Following are the client RPC APIs supported by the replica node in JsonRpcAccountsService.

- getAccountInfo
- getMultipleAccounts
- getProgramAccounts
- getMinimumBalanceForRentExemption
- getInflationGovenor
- getInflationRate
- getEpochSchedule
- getRecentBlockhash
- getFees
- getFeeCalculatorForBlockhash
- getFeeRateGovernor
- getLargestAccounts
- getSupply
- getStakeActivation
- getTokenAccountBalance
- getTokenSupply
- getTokenLargestAccounts
- getTokenAccountsByOwner
- getTokenAccountsByDelegate

Following APIs are not included:

- getInflationReward
- getEpochSchedule
- getClusterNodes
- getRecentPerformanceSamples
- getBlockCommitment
- getGenesisHash
- getSignatueStatuses
- getMaxRetransmitSlot
- getMaxShredInsertSlot
- sendTransaction
- simulateTransaction
- getSlotLeader
- getSlotLeaders
- minimumLedgerSlot
- getBlock
- getBlockTime
- getBlocks
- getBlocksWithLimit
- getTransaction
- getSignaturesForAddress
- getFirstAvailableBlock
- getBlockProduction
