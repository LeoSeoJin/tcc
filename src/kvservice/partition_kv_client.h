#ifndef SCC_KVSERVICE_PARTITION_KV_TX_CLIENT_H
#define SCC_KVSERVICE_PARTITION_KV_TX_CLIENT_H

#include "common/types.h"
#include "rpc/async_rpc_client.h"
#include "rpc/sync_rpc_client.h"
#include <string>
#include <vector>

namespace scc {

    class PartitionKVClient {
    public:
        PartitionKVClient(std::string serverName, int serverPort);

        ~PartitionKVClient();

        bool ShowItem(const std::string &key, std::string &itemVersions);

        void InitializePartitioning(DBPartition source); // done by a different connection

        void SendLST(PhysicalTimeSpec lst, int round);

        void SendGST(PhysicalTimeSpec gst);

#ifdef WREN

        void SendGST(PhysicalTimeSpec local, PhysicalTimeSpec remote);

        void SendLST(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int round);

#endif

        void TxSliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys, int src);
		
#ifdef CURE
		void SliceReadKeys(unsigned int txId, TxContex &cdata, const std::vector<std::string> &keys, int src);
        void PrepareRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                            std::vector<std::string> &values, int srcPartition);
        void WriteRequest(int txId, TxContex &cdata, const std::vector<std::string> &keys,
                            std::vector<std::string> &values, int srcPartition);
#endif

#ifdef TUNABLE_CAUSAL
		void TcSliceReadKeys(unsigned int txId, TxContex &cdata, int level, const std::vector<std::string> &keys, int src);
#endif

#if defined(H_CURE) || defined(WREN)

        void CommitRequest(unsigned int txId, PhysicalTimeSpec ct);

#elif defined(CURE)
        void CommitRequest(unsigned int txId, std::vector<PhysicalTimeSpec> ct);
#endif

#ifdef CURE
        template<class Result>
        void SendInternalSliceReadKeysResult(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
#ifndef TUNABLE_CAUSAL
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalSliceReadKeysResult, serializedArg);
#endif

#ifdef TUNABLE_CAUSAL
			_asyncRpcClient->CallWithNoState(RPCMethod::TcInternalSliceReadKeysResult, serializedArg);
#endif
        }
#endif
        template<class Result>
        void SendInternalTxSliceReadKeysResult(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalTxSliceReadKeysResult, serializedArg);
        }

        template<class Result>
        void SendPrepareReply(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalPrepareReply, serializedArg);
        }

        template<class Result>
        void SendWriteReply(Result &opResult) {
            std::string serializedArg = opResult.SerializeAsString();
            _asyncRpcClient->CallWithNoState(RPCMethod::InternalWriteReply, serializedArg);
        }

        /* Stabilization protocol */
#ifdef H_CURE

        void SendRST(PhysicalTimeSpec rst, int partitionId);

#endif

#ifdef WREN

        void SendStabilizationTimesToPeers(PhysicalTimeSpec lst, PhysicalTimeSpec rst, int partitionId);

#endif


#ifdef CURE
        void SendPVV(std::vector<PhysicalTimeSpec> pvv, int round);

        void SendGSV(std::vector<PhysicalTimeSpec> gsv);

#endif

    private:
        std::string _serverName;
        int _serverPort;
        AsyncRPCClient *_asyncRpcClient;
        SyncRPCClient *_syncRpcClient;



    };
}

#endif
